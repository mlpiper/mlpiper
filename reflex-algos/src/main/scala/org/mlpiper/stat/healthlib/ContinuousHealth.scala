package org.mlpiper.stat.healthlib

import com.parallelmachines.reflex.common.InfoType.InfoType
import org.apache.flink.streaming.scala.examples.common.stats.GlobalAccumulator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.mlpiper.datastructures.NamedMatrix
import org.mlpiper.stat.histogram.HistogramComparatorTypes
import org.mlpiper.stat.histogram.continuous.{CompareTwoFeaturedHistograms, CombineFeaturedHistograms => CombineFeaturedContinuousHistograms, Histogram => ContinuousHistogram, HistogramWrapper => ContinuousHistogramWrapper, NamedMatrixToFeaturedHistogram => NamedMatrixToFeaturedContinuousHistogram, OverlapResult => OverlapResultForContinuousHistogram}

import scala.collection.mutable

class ContinuousHealth(accumName: String)
  extends Serializable {

  //////////////////////////////////////////////////////////////////////////////////////
  // Spark - Batch Applications
  //////////////////////////////////////////////////////////////////////////////////////
  /**
    * Class is responsible for updating accumulator for featured histogram in Spark.
    */
  private class HistogramSparkAccumulatorUpdater(infoType: InfoType, modelId: String, sc: SparkContext) {

    private var globalHistStat: GlobalAccumulator[ContinuousHistogramWrapper] = _

    def updateStatAccumulator(hist: mutable.Map[String, ContinuousHistogram]): Unit = {
      if (globalHistStat != null) {
        globalHistStat.localUpdate(ContinuousHistogramWrapper(hist))
      } else {
        globalHistStat = NamedMatrixToFeaturedContinuousHistogram.getAccumulator(accumName, infoType, modelId, ContinuousHistogramWrapper(hist))
      }
      globalHistStat.updateSparkAccumulator(sc)
    }
  }

  /**
    * createHistogram function is responsible for calculating histogram on RDD of namedVector
    * Method gives access to named histogram generation on RDD.
    *
    * @param rddOfNamedMatrix                RDD of NamedMatrix
    * @param binSizeForEachFeatureForRef     Map specifying size requirement for each bins associated with each featureIDs
    * @param minBinValueForEachFeatureForRef Map specifying least values in bin associated with each featureIDs
    * @param maxBinValueForEachFeatureForRef Map specifying max values in bin associated with each featureIDs
    * @return A map containing the featureID and histogram object
    */
  def createHistogram(rddOfNamedMatrix: RDD[NamedMatrix],
                      binSizeForEachFeatureForRef: Option[Map[String, Double]],
                      minBinValueForEachFeatureForRef: Option[Map[String, Double]],
                      maxBinValueForEachFeatureForRef: Option[Map[String, Double]],
                      enableAccumOutputOfHistograms: Boolean,
                      scOption: Option[SparkContext],
                      infoType: InfoType,
                      modelId: String)
  : mutable.Map[String, ContinuousHistogram] = {


    val histogram = rddOfNamedMatrix
      // Convert each matrix into a histogram
      .map(x => NamedMatrixToFeaturedContinuousHistogram(x,
      binSizeForEachFeatureForRef = binSizeForEachFeatureForRef,
      minBinValueForEachFeatureForRef = minBinValueForEachFeatureForRef,
      maxBinValueForEachFeatureForRef = maxBinValueForEachFeatureForRef))
      // Merge each partition's histogram using a reduce
      .reduce((x, y) => CombineFeaturedContinuousHistograms.combineTwoFeaturedHistograms(x, y))

    // updating accumulator if it is enabled
    if (enableAccumOutputOfHistograms) {
      new HistogramSparkAccumulatorUpdater(infoType, modelId, sc = scOption.get).updateStatAccumulator(histogram)
    }

    histogram
  }

  def compareHistogram(contenderHistogram: mutable.Map[String, ContinuousHistogram],
                       inferenceHist: mutable.Map[String, ContinuousHistogram],
                       method: HistogramComparatorTypes.HistogramComparatorMethodType,
                       addAdjustmentNormalizingEdge: Boolean,
                       sc: SparkContext,
                       modelId: String): OverlapResultForContinuousHistogram = {

    val score = CompareTwoFeaturedHistograms
      .compare(contenderfeaturedHistogram = contenderHistogram,
        inferencefeaturedHistogram = inferenceHist,
        method = method,
        addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge)

    val inputHistogramRepresentation = contenderHistogram
    val contenderHistogramRepresentation = inferenceHist

    val overlapResult = new OverlapResultForContinuousHistogram(score = score,
      inputHistStream = inputHistogramRepresentation,
      contenderHistStream = contenderHistogramRepresentation)

    val healthAcc = OverlapResultForContinuousHistogram.getAccumulator(overlapResult, InfoType.HealthCompare, modelId)
    healthAcc.updateSparkAccumulator(sc)

    overlapResult
  }
}
