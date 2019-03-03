package org.mlpiper.stat.algos

import com.parallelmachines.reflex.common.enums.OpType
import com.parallelmachines.reflex.common.InfoType
import org.mlpiper.stats._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.mlpiper.datastructures.{ColumnEntry, NamedVector}
import org.mlpiper.stat.healthlib.HealthType
import org.mlpiper.stat.histogram.categorical.{CombineFeaturedHistograms => CombineFeaturedCategoricalHistograms, HistogramFormatting => CategoricalHistogramFormatting, HistogramWrapper => CategoricalHistogramWrapper, NamedMatrixToFeaturedHistogram => NamedMatrixToCategoricalFeaturedHistogram}
import org.mlpiper.utils.{GenericNamedMatrixUtils, ParsingUtils}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Class is responsible for providing stats based on SparkML inference stat or stat that might have collected during inference!
  */
class SparkMLClassificationStatForSpark extends Serializable {
  // holds prediction of type Any
  var predictionRDD: RDD[Any] = _
  var rawPredictionRDD: RDD[(Any, Any)] = _
  var predictionColName: String = ""
  var rawPredictionColName: String = ""
  var featuresColName: String = ""

  def setPredictionColName(predictionColName: String): Unit = {
    this.predictionColName = predictionColName
  }

  def setRawPredictionColName(rawPredictionColName: String): Unit = {
    this.rawPredictionColName = rawPredictionColName
  }

  def setFeaturesColName(featuresColName: String): Unit = {
    this.featuresColName = featuresColName
  }

  def setPredictionRDD(predictionRowsRDD: RDD[Row]): Unit = {
    this.predictionRDD = predictionRowsRDD.map(eachPrediction => eachPrediction.getAs[Any](this.predictionColName))
    if (this.rawPredictionColName != "") {
      this.rawPredictionRDD = predictionRowsRDD.map(eachPrediction => (eachPrediction.getAs[Any](this.predictionColName), eachPrediction.getAs[Any](this.rawPredictionColName)))
    }
  }

  def createStats(sc: SparkContext): Unit = {
    // creating prediction distribution from prediction
    SparkMLPredictionDistributionClassificationStat.createStat(predictionRDD = predictionRDD, predictionColName = this.predictionColName, sc = sc)
    if (this.rawPredictionColName != "") {
      SparkMLProbabilityClassificationStat.createStat(predictionRDD = rawPredictionRDD, rawPredictionColName = this.rawPredictionColName, predictionColName = this.predictionColName, sc = sc)
    }
  }
}

/**
  * Class is responsible for providing stats based on SparkML inference (for clustering algos) stat or stat that might have collected during inference!
  */
class SparkMLClusteringStatForSpark extends Serializable {
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
    // creating cluster distribution from prediction
    SparkMLPredictionDistributionClassificationStat.createStat(predictionRDD = predictionRDD, predictionColName = this.predictionColName, sc = sc)
    //ClusterAffinityDistributionStat.createStat(sc = sc)
  }
}

object SparkMLPredictionDistributionClassificationStat extends Serializable {

  /**
    * Class is responsible for updating stat for classification in Spark.
    */
  private class AccumulatorUpdater(sc: SparkContext) {

    private def getPredictionDistributionAccumulator(startingHist: CategoricalHistogramWrapper)
    : GlobalAccumulator[CategoricalHistogramWrapper] = {
      new GlobalAccumulator[CategoricalHistogramWrapper](
        // right now outputting to categorical until we include dataType in accums
        name = HealthType.PredictionHistogramHealth.toString,
        localMerge = (_: AccumulatorInfo[CategoricalHistogramWrapper],
                      newHist: AccumulatorInfo[CategoricalHistogramWrapper]) => {
          // local - compute-operator specific accumulator should only be replaced!
          newHist
        },
        globalMerge = (x: AccumulatorInfo[CategoricalHistogramWrapper],
                       y: AccumulatorInfo[CategoricalHistogramWrapper]) => {
          AccumulatorInfo(
            // global - accumulator should only be merged using reduce functionality!
            value = CategoricalHistogramWrapper(
              CombineFeaturedCategoricalHistograms.combineTwoFeaturedHistograms(x.value.histogram, y.value.histogram)),
            count = x.count + y.count,
            accumModeType = x.accumModeType,
            accumGraphType = x.accumGraphType,
            name = x.name,
            infoType = x.infoType)
        },
        startingValue = startingHist,
        accumDataType = AccumData.BarGraph,
        accumModeType = AccumMode.Instant,
        infoType = InfoType.InfoType.General
      )
    }

    private var globalPredictionDistributionStat: GlobalAccumulator[CategoricalHistogramWrapper] = _

    def updatePredictionDistributionAccumulator(histWrapper: CategoricalHistogramWrapper): Unit = {
      if (globalPredictionDistributionStat != null) {
        globalPredictionDistributionStat.localUpdate(histWrapper)
      } else {
        globalPredictionDistributionStat = getPredictionDistributionAccumulator(histWrapper)
      }
      globalPredictionDistributionStat.updateSparkAccumulator(sc)
    }
  }

  /**
    * Method will create prediction distribution for classification.
    */
  def createStat(predictionRDD: RDD[Any], predictionColName: String, sc: SparkContext): Unit = {
    val namedPredictionRDD = predictionRDD.map(eachPredictedLabel => {
      val rce = ColumnEntry(
        columnName = predictionColName,
        columnValue = eachPredictedLabel.toString,
        // classification problems are always creating categorical labels
        columnType = OpType.CATEGORICAL)
      NamedVector(vector = Array(rce))
    })

    val rddOfCategoricalPredictions = namedPredictionRDD.map(_.toCategoricalNamedVector()).filter(_.vector.nonEmpty)

    val rddOfNamedMatrix = GenericNamedMatrixUtils.createReflexNamedMatrix(rddOfNamedVector = rddOfCategoricalPredictions)

    if (!rddOfNamedMatrix.isEmpty()) {

      val histogram = rddOfNamedMatrix
        // Convert each matrix into a histogram
        .map(x => NamedMatrixToCategoricalFeaturedHistogram(x))
        // Merge each partition's histogram using a reduce
        .reduce((x, y) => CombineFeaturedCategoricalHistograms.combineTwoFeaturedHistograms(x, y))

      val finalHistogram = CategoricalHistogramFormatting.formatFeaturedHistogram(histogram,
        enableNormalization = true,
        setOfPredefinedCategoriesForFeatures = None)

      new AccumulatorUpdater(sc = sc).updatePredictionDistributionAccumulator(CategoricalHistogramWrapper(finalHistogram))
    } else {
      LoggerFactory.getLogger(getClass).info("Cannot create prediction distribution given no predictions are made!")
    }
  }
}


object SparkMLProbabilityClassificationStat extends Serializable {

  /** Winners Probability Stat Will hold all Mean, Max and Min of Probability For Classification Problems */
  case class WinnersProbabilityStatWrapper(winnersProbabilityStatMap: Map[String, Double]) {
    override def toString: String = {
      ParsingUtils.iterableToJSON(winnersProbabilityStatMap.map(x => (x._1.toString, x._2)))
    }

    def +(that: WinnersProbabilityStatWrapper): WinnersProbabilityStatWrapper = {
      val newWinnersProbabilityStatMap: mutable.Map[String, Double] = mutable.Map[String, Double]()

      newWinnersProbabilityStatMap(WinnerProbabilityMaxKey) =
        Math.max(this.winnersProbabilityStatMap(WinnerProbabilityMaxKey), that.winnersProbabilityStatMap(WinnerProbabilityMaxKey))

      newWinnersProbabilityStatMap(WinnerProbabilityMinKey) =
        Math.min(this.winnersProbabilityStatMap(WinnerProbabilityMinKey), that.winnersProbabilityStatMap(WinnerProbabilityMinKey))

      newWinnersProbabilityStatMap(WinnerProbabilityMeanKey) =
        (this.winnersProbabilityStatMap(WinnerProbabilityMeanKey) + that.winnersProbabilityStatMap(WinnerProbabilityMeanKey)) / 2

      WinnersProbabilityStatWrapper(winnersProbabilityStatMap = newWinnersProbabilityStatMap.toMap)
    }
  }

  /** All Category Probability Stat Will hold Mean Probability For Classification Problems */
  case class AllCategoryProbabilityStatWrapper(allCategoryProbabilityStat: Map[String, Double]) extends GraphFormat {
    override def toString: String = {
      val mapOfCatsDescAndCategoryProbabilityStat = Map[String, Any]("labels" -> this.toGraphJsonable())
      ParsingUtils.iterableToJSON(mapOfCatsDescAndCategoryProbabilityStat)
    }

    def +(that: AllCategoryProbabilityStatWrapper): AllCategoryProbabilityStatWrapper = {
      val newAllCategoryProbabilityStatWrapper: mutable.Map[String, Double] = mutable.Map[String, Double]()

      val eachCategories: Set[String] = this.allCategoryProbabilityStat.keySet ++ that.allCategoryProbabilityStat.keySet

      eachCategories.foreach(eachCat => {
        if (this.allCategoryProbabilityStat.contains(eachCat) && that.allCategoryProbabilityStat.contains(eachCat)) {
          newAllCategoryProbabilityStatWrapper.put(eachCat, (this.allCategoryProbabilityStat(eachCat) + that.allCategoryProbabilityStat(eachCat)) / 2)
        } else if (this.allCategoryProbabilityStat.contains(eachCat)) {
          newAllCategoryProbabilityStatWrapper.put(eachCat, this.allCategoryProbabilityStat(eachCat))
        } else {
          newAllCategoryProbabilityStatWrapper.put(eachCat, that.allCategoryProbabilityStat(eachCat))
        }
      })

      AllCategoryProbabilityStatWrapper(allCategoryProbabilityStat = newAllCategoryProbabilityStatWrapper.toMap)
    }

    override def getDataMap: Map[String, Double] = this.allCategoryProbabilityStat
  }

  private class AccumulatorUpdater(sc: SparkContext) {

    /** Method is responsible for providing accumulator for Winner Probability StatWrapper */
    private def getWinnersProbabilityStatAccumulator(winnersProbabilityStatWrapper: WinnersProbabilityStatWrapper)
    : GlobalAccumulator[WinnersProbabilityStatWrapper] = {
      new GlobalAccumulator[WinnersProbabilityStatWrapper](
        name = StatNames.predictionConfidenceClassificationStat(),
        // locally, WinnersProbabilityStatWrapper will be replaced
        localMerge = (_: AccumulatorInfo[WinnersProbabilityStatWrapper],
                      newWinnersProbabilityStatWrapper: AccumulatorInfo[WinnersProbabilityStatWrapper]) => {
          newWinnersProbabilityStatWrapper
        },
        globalMerge = (x: AccumulatorInfo[WinnersProbabilityStatWrapper],
                       y: AccumulatorInfo[WinnersProbabilityStatWrapper]) => {
          AccumulatorInfo(
            value = x.value.+(y.value),
            count = x.count + y.count,
            accumModeType = x.accumModeType,
            accumGraphType = x.accumGraphType,
            name = x.name,
            infoType = x.infoType)
        },
        startingValue = winnersProbabilityStatWrapper,
        accumDataType = AccumData.getGraphType(winnersProbabilityStatWrapper.winnersProbabilityStatMap),
        accumModeType = AccumMode.Instant,
        infoType = InfoType.InfoType.General
      )
    }

    private var globalStatForWinnerProbabilityStat: GlobalAccumulator[WinnersProbabilityStatWrapper] = _

    /** Method will update Winner Probability Stat in associated accumulator */
    def updateWinnerProbabilityStatAccumulator(winnersProbabilityStatWrapper: WinnersProbabilityStatWrapper): Unit = {
      if (globalStatForWinnerProbabilityStat != null) {
        globalStatForWinnerProbabilityStat.localUpdate(winnersProbabilityStatWrapper)
      } else {
        globalStatForWinnerProbabilityStat = getWinnersProbabilityStatAccumulator(winnersProbabilityStatWrapper)
      }

      globalStatForWinnerProbabilityStat.updateSparkAccumulator(sc)
    }

    /** Method is responsible for providing accumulator for Winner Probability StatWrapper */
    private def getAllCategoryProbabilityStatAccumulator(allCategoryProbabilityStatWrapper: AllCategoryProbabilityStatWrapper)
    : GlobalAccumulator[AllCategoryProbabilityStatWrapper] = {
      new GlobalAccumulator[AllCategoryProbabilityStatWrapper](
        name = StatNames.meanConfidencePerLabelClassificationStat(),
        // locally, AllCategoryProbabilityStatWrapper will be replaced
        localMerge = (_: AccumulatorInfo[AllCategoryProbabilityStatWrapper],
                      newAllCategoryProbabilityStatWrapper: AccumulatorInfo[AllCategoryProbabilityStatWrapper]) => {
          newAllCategoryProbabilityStatWrapper
        },
        globalMerge = (x: AccumulatorInfo[AllCategoryProbabilityStatWrapper],
                       y: AccumulatorInfo[AllCategoryProbabilityStatWrapper]) => {
          AccumulatorInfo(
            value = x.value.+(y.value),
            count = x.count + y.count,
            accumModeType = x.accumModeType,
            accumGraphType = x.accumGraphType,
            name = x.name,
            infoType = x.infoType)
        },
        startingValue = allCategoryProbabilityStatWrapper,
        accumDataType = AccumData.BarGraph,
        accumModeType = AccumMode.Instant,
        infoType = InfoType.InfoType.General
      )
    }

    private var globalStatForAllCategoriesMeanProbabilityStat: GlobalAccumulator[AllCategoryProbabilityStatWrapper] = _

    /** Method will update All Categories Probability Stat in associated accumulator */
    def updateAllCategoriesMeanProbabilityStatAccumulator(allCategoryProbabilityStatWrapper: AllCategoryProbabilityStatWrapper): Unit = {
      if (globalStatForAllCategoriesMeanProbabilityStat != null) {
        globalStatForAllCategoriesMeanProbabilityStat.localUpdate(allCategoryProbabilityStatWrapper)
      } else {
        globalStatForAllCategoriesMeanProbabilityStat = getAllCategoryProbabilityStatAccumulator(allCategoryProbabilityStatWrapper)
      }

      globalStatForAllCategoriesMeanProbabilityStat.updateSparkAccumulator(sc)
    }
  }

  private val WinnerProbabilityMeanKey = "mean"
  private val WinnerProbabilityMinKey = "min"
  private val WinnerProbabilityMaxKey = "max"


  def createStat(predictionRDD: RDD[(Any, Any)], rawPredictionColName: String, predictionColName: String, sc: SparkContext): Unit = {

    val winnerCatAndProbability = predictionRDD.map(eachRawPredictedLabel => {
      val probabilityVector = eachRawPredictedLabel._2.asInstanceOf[org.apache.spark.ml.linalg.DenseVector]
      val maxProbabilityVector = probabilityVector.values.max
      val sumProbabilityVector = probabilityVector.values.sum
      (maxProbabilityVector / sumProbabilityVector, eachRawPredictedLabel._1.toString)
    })
    val rddOfWinnerProbs = winnerCatAndProbability.map(_._1).map(x => org.apache.spark.mllib.linalg.Vectors.dense(x))
    val stats = Statistics.colStats(rddOfWinnerProbs)

    val winnerProbabilityStatMap: mutable.Map[String, Double] = mutable.Map[String, Double]()

    winnerProbabilityStatMap.put(WinnerProbabilityMeanKey, stats.mean.toArray.head)
    winnerProbabilityStatMap.put(WinnerProbabilityMaxKey, stats.max.toArray.head)
    winnerProbabilityStatMap.put(WinnerProbabilityMinKey, stats.min.toArray.head)

    val winnersProbabilityStatWrapper = WinnersProbabilityStatWrapper(winnersProbabilityStatMap = winnerProbabilityStatMap.toMap)

    new AccumulatorUpdater(sc = sc).updateWinnerProbabilityStatAccumulator(winnersProbabilityStatWrapper = winnersProbabilityStatWrapper)

    // in map, value will be tuple of (Double, Long) which will be represented by total accumulated probability and total # of it was predicted respectively!
    var winnerCategoryToAccumulatedProbMap: mutable.Map[String, (Double, Long)] = mutable.Map[String, (Double, Long)]().empty

    winnerCatAndProbability.collect().map(eachProbabilityAndAssociatedCat => { //TODO: change to map_reduce in the future
      val probabilityAndCount: (Double, Long) = if (winnerCategoryToAccumulatedProbMap.contains(eachProbabilityAndAssociatedCat._2)) {
        (eachProbabilityAndAssociatedCat._1 + winnerCategoryToAccumulatedProbMap(eachProbabilityAndAssociatedCat._2)._1
          , 1L + winnerCategoryToAccumulatedProbMap(eachProbabilityAndAssociatedCat._2)._2)
      } else {
        (eachProbabilityAndAssociatedCat._1, 1L)
      }
      winnerCategoryToAccumulatedProbMap.put(eachProbabilityAndAssociatedCat._2, probabilityAndCount)
    })
    val winnerCategoryToAverageProbs: mutable.Map[String, Double] = mutable.Map[String, Double]().empty

    winnerCategoryToAccumulatedProbMap.foreach(eachCatAndAccumProbAndCount => {
      val averageStat: Double = eachCatAndAccumProbAndCount._2._1 / eachCatAndAccumProbAndCount._2._2

      winnerCategoryToAverageProbs.put(eachCatAndAccumProbAndCount._1, averageStat)
    })

    if (winnerCategoryToAverageProbs.nonEmpty) {
      val allCategoryToAverageProbsStatWrapper = AllCategoryProbabilityStatWrapper(allCategoryProbabilityStat = winnerCategoryToAverageProbs.toMap)

      new AccumulatorUpdater(sc = sc).updateAllCategoriesMeanProbabilityStatAccumulator(allCategoryProbabilityStatWrapper = allCategoryToAverageProbsStatWrapper)
    }
  }
}
