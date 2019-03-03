package org.mlpiper.stat.algos

import com.parallelmachines.reflex.common.InfoType
import com.parallelmachines.reflex.common.enums.OpType
import org.mlpiper.stats._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.mlpiper.datastructures.{ColumnEntry, NamedVector}
import org.mlpiper.stat.healthlib.{ContinuousHistogramForSpark, HealthType, MinMaxRangeForHistogramType}
import org.mlpiper.stat.histogram.continuous.{CombineFeaturedHistograms => CombineFeaturedContinuousHistograms, HistogramWrapper => ContinuousHistogramWrapper, NamedMatrixToFeaturedHistogram => NamedMatrixToContinuousFeaturedHistogram}
import org.mlpiper.utils.{GenericNamedMatrixUtils, ParsingUtils}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Class is responsible for providing stats based on SparkML inference stat or stat that might have collected during inference!
  */
class SparkMLRegressionStatForSpark extends Serializable {
  // holds prediction of type Any
  var predictionRDD: RDD[Any] = _
  var predictionColName: String = ""
  var featuresColName: String = ""

  def setPredictionColName(predictionColName: String): Unit = {
    this.predictionColName = predictionColName
  }

  def setFeaturesColName(featuresColName: String): Unit = {
    this.featuresColName = featuresColName
  }

  def setPredictionRDD(predictionRowsRDD: RDD[Row]): Unit = {
    this.predictionRDD = predictionRowsRDD.map(eachPrediction => eachPrediction.getAs[Any](this.predictionColName))
  }

  def createStats(sc: SparkContext): Unit = {
    SparkMLPredictionDistributionRegressionStat.createStat(predictionRDD = predictionRDD, predictionColName = this.predictionColName, sc = sc)
    SparkMLPredictionRangeRegressionStat.createStat(predictionRDD = predictionRDD, predictionColName = this.predictionColName, sc = sc)
  }
}

object SparkMLPredictionDistributionRegressionStat extends Serializable {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Class is responsible for updating stat for classification in Spark.
    */
  private class AccumulatorUpdater(sc: SparkContext) {
    private def getPredictionDistributionAccumulator(startingHist: ContinuousHistogramWrapper)
    : GlobalAccumulator[ContinuousHistogramWrapper] = {
      new GlobalAccumulator[ContinuousHistogramWrapper](
        name = HealthType.PredictionHistogramHealth.toString,
        localMerge = (_: AccumulatorInfo[ContinuousHistogramWrapper],
                      newHist: AccumulatorInfo[ContinuousHistogramWrapper]) => {
          // local - compute-operator specific accumulator should only be replaced!
          newHist
        },
        globalMerge = (x: AccumulatorInfo[ContinuousHistogramWrapper],
                       y: AccumulatorInfo[ContinuousHistogramWrapper]) => {
          AccumulatorInfo(
            // global - accumulator should only be merged using reduce functionality!
            value = ContinuousHistogramWrapper(
              CombineFeaturedContinuousHistograms.combineTwoFeaturedHistograms(x.value.histogram, y.value.histogram)),
            count = x.count + y.count,
            accumModeType = x.accumModeType,
            accumGraphType = x.accumGraphType,
            name = x.name,
            infoType = x.infoType)
        },
        startingValue = startingHist,
        accumDataType = AccumData.BarGraph,
        accumModeType = AccumMode.Instant,
        InfoType.InfoType.General
      )
    }

    private var globalPredictionDistributionStat: GlobalAccumulator[ContinuousHistogramWrapper] = _

    def updatePredictionPredictionDistributionAccumulator(histWrapper: ContinuousHistogramWrapper): Unit = {
      if (globalPredictionDistributionStat != null) {
        globalPredictionDistributionStat.localUpdate(histWrapper)
      } else {
        globalPredictionDistributionStat = getPredictionDistributionAccumulator(histWrapper)
      }
      globalPredictionDistributionStat.updateSparkAccumulator(sc)
    }
  }

  /**
    * Method will create prediction distribution for regression.
    */
  def createStat(predictionRDD: RDD[Any], predictionColName: String, sc: SparkContext): Unit = {
    val namedPredictionRDD = predictionRDD.map(eachPredictedLabel => {
      val rce = ColumnEntry(
        columnName = predictionColName,
        columnValue = eachPredictedLabel,
        // regression problems are always creating continuous labels
        columnType = OpType.CONTINUOUS)
      NamedVector(vector = Array(rce))
    })

    val rddOfContinuousPredictions = namedPredictionRDD.map(_.toContinuousNamedVector()).filter(_.vector.nonEmpty)
    val regressionStatAccumulatorUpdater = new AccumulatorUpdater(sc = sc)

    val rddOfNamedMatrix = GenericNamedMatrixUtils.createReflexNamedMatrix(rddOfNamedVector = rddOfContinuousPredictions)

    if (!rddOfNamedMatrix.isEmpty()) {
      val tup =
        ContinuousHistogramForSpark
          .getMinMaxBinRange(rddOfNamedVectorNumerics = rddOfContinuousPredictions,
            minBinValueForEachFeatureForRef = None,
            maxBinValueForEachFeatureForRef = None,
            minMaxRangeType = MinMaxRangeForHistogramType.ByMinMax
          )

      val minBinValueForEachFeature = tup._1
      val maxBinValueForEachFeature = tup._2

      val histogram = rddOfNamedMatrix
        // Convert each matrix into a histogram
        .map(x => NamedMatrixToContinuousFeaturedHistogram(x,
        binSizeForEachFeatureForRef = None,
        minBinValueForEachFeatureForRef = minBinValueForEachFeature,
        maxBinValueForEachFeatureForRef = maxBinValueForEachFeature))
        // Merge each partition's histogram using a reduce
        .reduce((x, y) => CombineFeaturedContinuousHistograms.combineTwoFeaturedHistograms(x, y))

      regressionStatAccumulatorUpdater.updatePredictionPredictionDistributionAccumulator(ContinuousHistogramWrapper(histogram))

    } else {
      logger.info("Cannot create prediction distribution given no predictions are made!")
    }
  }
}

object SparkMLPredictionRangeRegressionStat extends Serializable {
  private val logger = LoggerFactory.getLogger(getClass)

  private class AccumulatorUpdater(sc: SparkContext) {
    /** Method is responsible for providing accumulator for RegressionPredictionStatWrapper */
    private def getRegressionPredictionStatAccumulator(regressionPredictionStatWrapper: RegressionPredictionStatWrapper)
    : GlobalAccumulator[RegressionPredictionStatWrapper] = {
      new GlobalAccumulator[RegressionPredictionStatWrapper](
        name = StatNames.PredictionRangeRegressionStat(),
        // locally, RegressionPredictionStatWrapper will be replaced
        localMerge = (_: AccumulatorInfo[RegressionPredictionStatWrapper],
                      newRegressionPredictionStatWrapper: AccumulatorInfo[RegressionPredictionStatWrapper]) => {
          newRegressionPredictionStatWrapper
        },
        globalMerge = (x: AccumulatorInfo[RegressionPredictionStatWrapper],
                       y: AccumulatorInfo[RegressionPredictionStatWrapper]) => {
          AccumulatorInfo(
            // global - accumulator should only be merged using reduce functionality! Since it will be operators overloaded, we have to rely on given reduce functionality
            value = x.value.+(y.value),
            count = x.count + y.count,
            accumModeType = x.accumModeType,
            accumGraphType = x.accumGraphType,
            name = x.name,
            infoType = x.infoType)
        },
        startingValue = regressionPredictionStatWrapper,
        accumDataType = AccumData.getGraphType(regressionPredictionStatWrapper.regressionPredictionStatMap),
        accumModeType = AccumMode.Instant,
        infoType = InfoType.InfoType.General
      )
    }

    private var globalStatForRegressionPredictionStat: GlobalAccumulator[RegressionPredictionStatWrapper] = _

    /** Method will update regression Prediction Stat in associated accumulator */
    def updateRegressionPredictionStatAccumulator(regressionPredictionStatWrapper: RegressionPredictionStatWrapper): Unit = {
      if (globalStatForRegressionPredictionStat != null) {
        globalStatForRegressionPredictionStat.localUpdate(regressionPredictionStatWrapper)
      } else {
        globalStatForRegressionPredictionStat = getRegressionPredictionStatAccumulator(regressionPredictionStatWrapper)

      }
      globalStatForRegressionPredictionStat.updateSparkAccumulator(sc)
    }
  }

  private val RegressionStatMinKey = "min"
  private val RegressionStatMaxKey = "max"
  private val RegressionStatMeanKey = "mean"

  /** RegressionPredictionStatWrapper will hold all Mean, Max and Min of Labels For Regression Problems */
  case class RegressionPredictionStatWrapper(regressionPredictionStatMap: Map[String, Double]) {
    override def toString: String = {
      ParsingUtils.iterableToJSON(regressionPredictionStatMap.map(x => (x._1.toString, x._2)))
    }

    def +(that: RegressionPredictionStatWrapper): RegressionPredictionStatWrapper = {
      val newRegressionPredictionStatMap: mutable.Map[String, Double] = mutable.Map[String, Double]()

      newRegressionPredictionStatMap(RegressionStatMaxKey) =
        Math.max(this.regressionPredictionStatMap(RegressionStatMaxKey), that.regressionPredictionStatMap(RegressionStatMaxKey))

      newRegressionPredictionStatMap(RegressionStatMinKey) =
        Math.min(this.regressionPredictionStatMap(RegressionStatMinKey), that.regressionPredictionStatMap(RegressionStatMinKey))

      newRegressionPredictionStatMap(RegressionStatMeanKey) =
        (this.regressionPredictionStatMap(RegressionStatMeanKey) + that.regressionPredictionStatMap(RegressionStatMeanKey)) / 2

      RegressionPredictionStatWrapper(regressionPredictionStatMap = newRegressionPredictionStatMap.toMap)
    }
  }

  def createStat(predictionRDD: RDD[Any], predictionColName: String, sc: SparkContext): Unit = {
    val namedPredictionRDD = predictionRDD.map(eachPredictedLabel => {
      val rce = ColumnEntry(
        columnName = predictionColName,
        columnValue = eachPredictedLabel,
        // regression problems are always creating continuous labels
        columnType = OpType.CONTINUOUS)
      NamedVector(vector = Array(rce))
    })

    val rddOfContinuousPredictions = namedPredictionRDD.map(_.toContinuousNamedVector()).filter(_.vector.nonEmpty)
    val rddVector = rddOfContinuousPredictions
      .map(_.vector.map(_.columnValue.asInstanceOf[Double]))
      .map(x => org.apache.spark.mllib.linalg.Vectors.dense(x))
    val regressionPredictionStatMap: mutable.Map[String, Double] = mutable.Map[String, Double]()

    if (!rddVector.isEmpty) {
      val stats = Statistics.colStats(rddVector)
      val meanHead = stats.mean.toArray.head
      val maxHead = stats.max.toArray.head
      val minHead = stats.min.toArray.head

      regressionPredictionStatMap.put(RegressionStatMeanKey, meanHead)
      regressionPredictionStatMap.put(RegressionStatMaxKey, maxHead)
      regressionPredictionStatMap.put(RegressionStatMinKey, minHead)
    } else {
      logger.warn("Unable to make distribution statistics for non-continuous predictions")
    }

    val regressionPredictionStatWrapper = RegressionPredictionStatWrapper(regressionPredictionStatMap = regressionPredictionStatMap.toMap)

    new AccumulatorUpdater(sc = sc).updateRegressionPredictionStatAccumulator(regressionPredictionStatWrapper = regressionPredictionStatWrapper)
  }
}

