package org.mlpiper.stat.healthlib

import com.parallelmachines.reflex.common.InfoType.InfoType
import org.apache.flink.streaming.scala.examples.common.stats.GlobalAccumulator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.mlpiper.datastructures.NamedMatrix
import org.mlpiper.stat.histogram.HistogramComparatorTypes
import org.mlpiper.stat.histogram.categorical.{CombineFeaturedHistograms => CombineFeaturedCategoricalHistograms, CompareTwoFeaturedHistograms => CompareTwoFeaturedCategoricalHistograms, Histogram => CategoricalHistogram, HistogramFormatting => CategoricalHistogramFormatting, HistogramWrapper => CategoricalHistogramWrapper, NamedMatrixToFeaturedHistogram => NamedMatrixToFeaturedCategoricalHistogram, OverlapResult => OverlapResultForCategoricalHistogram}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class CategoricalHealth(accumName: String) extends Serializable {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  //////////////////////////////////////////////////////////////////////////////////////
  // Spark - Batch Applications
  //////////////////////////////////////////////////////////////////////////////////////
  /**
    * Class is responsible for updating accumulator for featured histogram in Spark.
    */
  private class HistogramSparkAccumulatorUpdater(infoType: InfoType, modelId: String, sc: SparkContext) {

    private var globalHistStat: GlobalAccumulator[CategoricalHistogramWrapper] = _

    def updateStatAccumulator(hist: mutable.Map[String, CategoricalHistogram]): Unit = {
      if (globalHistStat != null) {
        globalHistStat.localUpdate(CategoricalHistogramWrapper(hist))
      } else {
        globalHistStat = NamedMatrixToFeaturedCategoricalHistogram.getAccumulator(accumName, infoType, modelId, CategoricalHistogramWrapper(hist))
      }
      globalHistStat.updateSparkAccumulator(sc)
    }
  }

  /**
    * createHistogram function is responsible for calculating histogram on RDD of namedVector
    * Method gives access to named histogram generation on RDD.
    * Method will generate histograms for Categorical features.
    * User of API needs to make sure that given NamedMatrix contains only categorical data.
    *
    * @param rddOfNamedMatrix RDD of NamedMatrix
    * @return A map containing the featureID and histogram object
    */
  def createHistogram(rddOfNamedMatrix: RDD[NamedMatrix],
                      enableAccumOutputOfHistograms: Boolean,
                      setOfPredefinedCategoriesForFeatures: Option[Map[String, Set[String]]],
                      sc: SparkContext,
                      infoType: InfoType,
                      modelId: String)
  : mutable.Map[String, CategoricalHistogram] = {
    val histogram = rddOfNamedMatrix
      // Convert each matrix into a histogram
      .map(x => NamedMatrixToFeaturedCategoricalHistogram(x))
      // Merge each partition's histogram using a reduce
      .reduce((x, y) => CombineFeaturedCategoricalHistograms.combineTwoFeaturedHistograms(x, y))

    val finalHistogram = CategoricalHistogramFormatting.formatFeaturedHistogram(histogram,
      enableNormalization = true,
      setOfPredefinedCategoriesForFeatures = setOfPredefinedCategoriesForFeatures)


    // updating accumulator if it is enabled
    if (enableAccumOutputOfHistograms) {
      new HistogramSparkAccumulatorUpdater(infoType, modelId, sc = sc).updateStatAccumulator(finalHistogram)
    }

    finalHistogram
  }


  def compareHistogram(contenderHistogram: mutable.Map[String, CategoricalHistogram],
                       inferringHist: mutable.Map[String, CategoricalHistogram],
                       method: HistogramComparatorTypes.HistogramComparatorMethodType,
                       addAdjustmentNormalizingEdge: Boolean,
                       sc: SparkContext,
                       modelId: String): OverlapResultForCategoricalHistogram = {
    val inputHistogramRepresentation = contenderHistogram
    val contenderHistogramRepresentation = inferringHist

    val score = CompareTwoFeaturedCategoricalHistograms
      .compare(contenderFeatureHist = contenderHistogram,
        inferringFeatureHist = inferringHist,
        method = method,
        addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge)

    val overlapResult = new OverlapResultForCategoricalHistogram(score = score,
      inputHistStream = inputHistogramRepresentation,
      contenderHistStream = contenderHistogramRepresentation)

    val healthAcc = OverlapResultForCategoricalHistogram.getAccumulator(overlapResult, InfoType.HealthCompare, modelId)
    healthAcc.updateSparkAccumulator(sc)

    overlapResult
  }
}
