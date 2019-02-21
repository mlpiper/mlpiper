package org.apache.flink.streaming.scala.examples.clustering.stat.continuous

import com.parallelmachines.reflex.common.InfoType._
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.scala.examples.clustering.math.ReflexNamedMatrix
import org.apache.flink.streaming.scala.examples.clustering.stat.HistogramComparatorTypes
import org.apache.flink.streaming.scala.examples.common.stats.GlobalAccumulator
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * Compute Overlap on DataStreams of Featured Histograms
  *
  * functionality works in following way.
  *
  * <br> 1. Update contenderMap using flatMap1 method as soon as contender featured histogram comes in
  * <br> 2. flatMap2 will generate Histogram from DenseMatrix.
  * If contender histogram is available then new histogram will be in range as same as contender histogram
  * Else, range will be min and max of given feature
  * <br> 3. Using contenderMap, calculate score using associated method for input dataStream of featured histogram
  * <br> 4. If, contenderMap has not been set yet, then the score will not get outputted
  */
class GenerateHistogramAndCalculateOverlap(accName: String,
                                           var overlapType: String,
                                           modelId: String,
                                           addAdjustmentNormalizingEdge: Boolean,
                                           outputGeneratedHistogramToAccum: Boolean)
  extends RichCoFlatMapFunction[mutable.Map[String, Histogram],
    ReflexNamedMatrix,
    OverlapResult] {
  // contenderMap holds featured histogram that will be used for comparison
  var contenderMap: Option[mutable.Map[String, Histogram]] = None
  var contenderHistogramRepresentation: Option[mutable.Map[String, Histogram]] = None

  var minBinValueForEachFeatureForRef: Option[Map[String, Double]] = None
  var maxBinValueForEachFeatureForRef: Option[Map[String, Double]] = None
  var binSizeForEachFeatureForRef: Option[Map[String, Double]] = None

  private var globalStatForOverlapResult: GlobalAccumulator[OverlapResult] = _

  private var globalStatForFeaturedHistogram: GlobalAccumulator[HistogramWrapper] = _

  /** Method will update flink accumulator containing overlap results */
  def updateStatAccumulator(overlapResult: OverlapResult): Unit = {
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
      globalStatForFeaturedHistogram = NamedMatrixToFeaturedHistogram.getAccumulator(accName, InfoType.Health, null, HistogramWrapper(hist))
    }
    globalStatForFeaturedHistogram.updateFlinkAccumulator(this.getRuntimeContext)
  }

  /**
    * flatMap1 is associated with updating contenderMap
    * It is also responsible for processing unprocessed input featured hist as contenderMap is available now
    * Map representation has integer key and it will always start with 0.
    */
  override def flatMap1(contender: mutable.Map[String, Histogram],
                        out: Collector[OverlapResult]): Unit = {
    contenderMap = Some(contender)
    contenderHistogramRepresentation = Some(contender)

    // updating reference min and max bin ranges of each features
    val (minBinValueForEachFeature, maxBinValueForEachFeature, binSizeForEachFeature) = NamedMatrixToFeaturedHistogram.generateMinMaxRangeAndBinSizeFromFeaturedHist(contender)

    minBinValueForEachFeatureForRef = Some(minBinValueForEachFeature.toMap)
    maxBinValueForEachFeatureForRef = Some(maxBinValueForEachFeature.toMap)
    binSizeForEachFeatureForRef = Some(binSizeForEachFeature.toMap)
  }

  /**
    * flatMap2 is associated with input Dense Matrix
    * It will create input featured histogram by keeping contender histogram as a reference.
    * It is also responsible for calculating score by comparing contenderMap if contenderMap is set.
    */
  override def flatMap2(matrix: ReflexNamedMatrix,
                        out: Collector[OverlapResult]): Unit = {
    val featuredHistogram: mutable.Map[String, Histogram] =
      NamedMatrixToFeaturedHistogram(namedMatrixRep = matrix,
        binSizeForEachFeatureForRef = binSizeForEachFeatureForRef,
        minBinValueForEachFeatureForRef = minBinValueForEachFeatureForRef,
        maxBinValueForEachFeatureForRef = maxBinValueForEachFeatureForRef)

    // updating accumulator by outputting input histogram if flag is set
    if (outputGeneratedHistogramToAccum) {
      this.updateStatAccumulator(featuredHistogram)
    }

    // generating score only if we have contender histogram
    if (contenderMap.isDefined) {
      // calculating score using CompareTwoFeaturedHistograms
      val score: Map[String, Double] =
        CompareTwoFeaturedHistograms
          .compare(contenderfeaturedHistogram = contenderMap.get,
            inferencefeaturedHistogram = featuredHistogram,
            method = HistogramComparatorTypes.withName(overlapType),
            addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge)

      val inputHistogramRepresentation: mutable.Map[String, Histogram] = featuredHistogram

      val overlapResult = new OverlapResult(score = score,
        inputHistStream = inputHistogramRepresentation,
        contenderHistStream = contenderHistogramRepresentation.get)

      this.updateStatAccumulator(overlapResult = overlapResult)

      out.collect(overlapResult)
    }
  }
}
