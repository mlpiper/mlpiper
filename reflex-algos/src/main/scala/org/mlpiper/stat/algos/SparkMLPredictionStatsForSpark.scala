package org.mlpiper.stat.algos

import org.apache.flink.streaming.scala.examples.common.stats.{StatInfo, StatNames, StatPolicy}
import org.apache.flink.streaming.scala.examples.flink.utils.functions.performance.{PerformanceMetricsHash, PrintIntervalPerformanceMetrics}
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification._
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.{IndexToString, StringIndexerModel}
import org.apache.spark.ml.{PipelineModel, PredictionModel}
import org.apache.spark.sql.DataFrame
import org.mlpiper.parameters.performance.PerformanceMetrics
import org.slf4j.LoggerFactory

import scala.collection.mutable

class SparkMLPredictionStatsForSpark
  extends PerformanceMetrics[SparkMLPredictionStatsForSpark] with PrintIntervalPerformanceMetrics {
  var predictionDF: DataFrame = _
  var labelColName = ""
  var predictionColName = ""

  def getPredictionColName(): String = {
    this.predictionColName
  }


  def setPredictionDF(predictionDF: DataFrame): Unit = {
    this.predictionDF = predictionDF
  }

  /**
    * createStat function is responsible for calculating SparkML Stat on RDD of Any (predicted label)
    * And other relevant stats which are collected in associated mining object
    * Method will generate prediction distribution for Categorical/Continuous labels depending on
    * algorithm type too.
    *
    * @param model SparkML Pipeline Model
    * @return A map containing the featureID and histogram object
    */
  def createStat(model: PipelineModel,
                 sc: SparkContext): DataFrame = {
    val pipelineStage = model.stages(model.stages.length - 1)
    var featuresColName = ""
    var predictionMode = ""
    var rawPredictionColName = ""

    printMetricsIfTriggered()
    globalStats.values.foreach(x => x.updateSparkAccumulator(sc))


    pipelineStage match {
      case i: ProbabilisticClassificationModel[_, _] =>
        val stageModel = i
        this.predictionColName = stageModel.getPredictionCol
        featuresColName = stageModel.getFeaturesCol
        this.labelColName = stageModel.getLabelCol
        predictionMode = "classification"
        rawPredictionColName = stageModel.getProbabilityCol
      //stageModel.explainParams() TODO: provide parameters of the model
      case i: ClassificationModel[_, _] =>
        val stageModel = i
        this.predictionColName = stageModel.getPredictionCol
        featuresColName = stageModel.getFeaturesCol
        this.labelColName = stageModel.getLabelCol
        predictionMode = "classification"
        rawPredictionColName = stageModel.getRawPredictionCol
      //stageModel.explainParams() TODO: provide parameters of the model
      case i: GBTClassificationModel =>
        val stageModel = i
        this.predictionColName = stageModel.getPredictionCol
        featuresColName = stageModel.getFeaturesCol
        predictionMode = "classification"
        this.labelColName = stageModel.getLabelCol

      case i: KMeansModel =>
        val stageModel = i
        this.predictionColName = stageModel.getPredictionCol
        featuresColName = stageModel.getFeaturesCol
        predictionMode = "clustering"
      //rawPredictionColName = stageModel.getRawPredictionCol TODO compute the equivalent distances?
      //stageModel.clusterCenters TODO Provide cluster centers also provide distances between them
      //stageModel.summary() TODO: provide summary of the model
      // stageModel.computeCost(this.predictionDF) TODO: provide sum of squared error


      case i: PredictionModel[_, _] =>
        val stageModel = i
        this.predictionColName = stageModel.getPredictionCol
        featuresColName = stageModel.getFeaturesCol
        //stageModel.explainParams() TODO: provide summary of the model
        predictionMode = "regression"

      case _ =>
    }


    if (predictionMode == "classification") {
      setClassLabelNames(model = model, sc = sc)
      val csForSpark = new SparkMLClassificationStatForSpark()
      csForSpark.setPredictionColName(this.predictionColName)
      csForSpark.setFeaturesColName(featuresColName)
      csForSpark.setRawPredictionColName(rawPredictionColName)
      csForSpark.setPredictionRDD(this.predictionDF.rdd)
      csForSpark.createStats(sc = sc)
    } else if (predictionMode == "regression") {
      val rsForSpark = new SparkMLRegressionStatForSpark()
      rsForSpark.setPredictionColName(this.predictionColName)
      rsForSpark.setFeaturesColName(featuresColName)
      rsForSpark.setPredictionRDD(this.predictionDF.rdd)
      rsForSpark.createStats(sc = sc)
    } else if (predictionMode == "clustering") {
      val clusteringForSpark = new SparkMLClusteringStatForSpark()
      clusteringForSpark.setPredictionColName(this.predictionColName)
      clusteringForSpark.setFeaturesColName(featuresColName)
      clusteringForSpark.setPredictionRDD(this.predictionDF.rdd)
      clusteringForSpark.createStats(sc = sc)
    }
    this.predictionDF
  }

  def setClassLabelNames(model: PipelineModel, sc: SparkContext): Unit = {
    var labelValues = Array[String]()
    var isCategoricalLabel = false
    for (stages <- model.stages) {
      stages match {
        case i: StringIndexerModel =>
          val stageModel = i
          if (stageModel.getOutputCol == this.labelColName) {
            labelValues = stageModel.labels
            isCategoricalLabel = true
          }
        case _ =>
      }
    }
    if (isCategoricalLabel) {
      LoggerFactory.getLogger(getClass).debug(s"labelValues = $labelValues")
      this.predictionDF = this.predictionDF.withColumnRenamed(this.predictionColName, this.predictionColName + "_index")
      val indexToString = new IndexToString()
      indexToString.setLabels(labelValues)
        .setInputCol(this.predictionColName + "_index")
        .setOutputCol(this.predictionColName)

      val transDF = indexToString.transform(this.predictionDF)
      this.predictionDF = transDF
    }
  }

  override protected def storeMetrics(): Option[PerformanceMetricsHash] = {

    val perfHash = PerformanceMetricsHash(mutable.HashMap[StatInfo, Any](
      StatInfo(StatNames.Count, StatPolicy.SUM) -> this.predictionDF.count
    ))

    Some(perfHash)
  }

}
