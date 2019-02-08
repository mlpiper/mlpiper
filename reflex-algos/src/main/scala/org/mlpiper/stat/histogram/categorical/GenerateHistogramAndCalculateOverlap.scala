package org.mlpiper.stat.histogram.categorical

import com.parallelmachines.reflex.common.InfoType._
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.scala.examples.common.stats.GlobalAccumulator
import org.apache.flink.util.Collector
import org.mlpiper.datastructures.NamedMatrix
import org.mlpiper.stat.histogram.HistogramComparatorTypes

import scala.collection.mutable

/**
  * Compute Overlap on DataStreams of Featured Histograms
  *
  * functionality works in following way.
  *
  * <br> 1. Update contenderMap using flatMap1 method as soon as contender featured histogram comes in
  * <br> 2. Using flatMap2, create histogram of input streams. It will generate histograms of limited categories only if contenders' histogram is available.
  * <br> 2. Using contenderMap, calculate score using associated method for input dataStream of featured histogram
  * <br> 3. If, contenderMap has not been set yet, then the score will not get outputted
  */
class GenerateHistogramAndCalculateOverlap(accumName: String,
                                           modelId: String,
                                           outputGeneratedHistogramToAccum: Boolean,
                                           method: HistogramComparatorTypes.HistogramComparatorMethodType,
                                           addAdjustmentNormalizingEdge: Boolean)
  extends RichCoFlatMapFunction[
    // contender histogram
    mutable.Map[String, Histogram],
    // inferring stream  of NamedMatrix
    NamedMatrix,
    OverlapResult] {
  // contenderMap holds featured histogram that will be used for comparison
  var contenderMap: Option[mutable.Map[String, Histogram]] = None
  // map of fixed categories for each features
  var setOfPredefinedCategoriesForFeatures: Option[Map[String, Set[String]]] = None

  private var globalStatForOverlapResult: GlobalAccumulator[OverlapResult] = _

  private var globalStatForFeaturedHistogram: GlobalAccumulator[HistogramWrapper] = _

  /** Method will update flink accumulator containing overlap results */
  def updateStatAccumulator(overlapResult: OverlapResult, modelId: String): Unit = {
    if (globalStatForOverlapResult != null) {
      globalStatForOverlapResult.localUpdate(overlapResult)
    } else {
      globalStatForOverlapResult = OverlapResult.getAccumulator(overlapResult, InfoType.HealthCompare, modelId)
    }
    globalStatForOverlapResult.updateFlinkAccumulator(this.getRuntimeContext)
  }

  /** Method will update flink accumulator containing histograms */
  def updateStatAccumulator(hist: mutable.Map[String, Histogram]): Unit = {
    if (globalStatForFeaturedHistogram != null) {
      globalStatForFeaturedHistogram.localUpdate(HistogramWrapper(hist))
    } else {
      globalStatForFeaturedHistogram = NamedMatrixToFeaturedHistogram.getAccumulator(accumName, InfoType.Health, modelId, HistogramWrapper(hist))
    }
    globalStatForFeaturedHistogram.updateFlinkAccumulator(this.getRuntimeContext)
  }

  /**
    * flatMap1 is associated with updating contenderMap
    * It is also responsible for processing unprocessed input featured hist as contenderMap is available now
    * Map representation has string key.
    */
  override def flatMap1(contender: mutable.Map[String, Histogram],
                        out: Collector[OverlapResult]): Unit = {
    contenderMap = Some(contender)

    setOfPredefinedCategoriesForFeatures = Some(HistogramFormatting.getFeaturedCategoryMap(contender))
  }

  /**
    * flatMap2 is associated with inferring histograms
    * It is responsible for calculating score by comparing contenderMap if contenderMap is set.
    */
  override def flatMap2(inferringStreamOfMatrix: NamedMatrix,
                        out: Collector[OverlapResult]): Unit = {
    val inferredFeaturedHistogram: mutable.Map[String, Histogram] =
      NamedMatrixToFeaturedHistogram(namedMatrixRep = inferringStreamOfMatrix)

    val formattedInferringFeaturedHistogram =
      HistogramFormatting.formatFeaturedHistogram(inferredFeaturedHistogram,
        enableNormalization = true,
        setOfPredefinedCategoriesForFeatures = setOfPredefinedCategoriesForFeatures)

    // updating accumulator by outputting input histogram if flag is set
    if (outputGeneratedHistogramToAccum) {
      this.updateStatAccumulator(formattedInferringFeaturedHistogram)
    }

    // generating score only if we have contender histogram
    if (contenderMap.isDefined) {
      // calculating score using CompareTwoFeaturedHistograms
      val score: Map[String, Double] = CompareTwoFeaturedHistograms
        .compare(inferringFeatureHist = formattedInferringFeaturedHistogram,
          contenderFeatureHist = contenderMap.get,
          method = method,
          addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge)

      val inferringHistogramRepresentation: mutable.Map[String, Histogram] = formattedInferringFeaturedHistogram
      val contenderHistogramRepresentation: mutable.Map[String, Histogram] = contenderMap.get

      val overlapResult = new OverlapResult(score = score,
        inputHistStream = inferringHistogramRepresentation,
        contenderHistStream = contenderHistogramRepresentation)

      this.updateStatAccumulator(overlapResult = overlapResult, modelId = modelId)

      out.collect(overlapResult)
    }
  }
}
