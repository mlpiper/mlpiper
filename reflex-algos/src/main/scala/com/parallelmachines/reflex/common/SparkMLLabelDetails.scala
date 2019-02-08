package com.parallelmachines.reflex.common

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{ClassificationModel, GBTClassificationModel}
import org.apache.spark.ml.feature.StringIndexerModel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * This object fetches label information form sparkML models
  */
object SparkMLLabelDetails {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * Function is responsible for providing labels transformation details.
    */
  def getLabelTransformationDetails(pipelineModel: PipelineModel): Option[Map[Double, String]] = {
    val pipelineLen = pipelineModel.stages.length - 1
    val lastPipelineStage = pipelineModel.stages(pipelineLen)
    var featuresColName = ""
    var labelColName = ""
    var predictionColName = ""
    var numClasses = 2
    var labelValues = Array[String]()
    val labelTransformationDetail: mutable.Map[Double, String] = mutable.Map[Double, String]()

    lastPipelineStage match {
      case i: ClassificationModel[_, _] =>
        val stageModel = i
        featuresColName = stageModel.getFeaturesCol
        labelColName = stageModel.getLabelCol
        predictionColName = stageModel.getPredictionCol
        numClasses = stageModel.numClasses.toInt
      case i: GBTClassificationModel =>
        val stageModel = i
        featuresColName = stageModel.getFeaturesCol
        labelColName = stageModel.getLabelCol
        predictionColName = stageModel.getPredictionCol
        numClasses = 2
      case _ =>
    }
    var isCategoricalLabel = false
    for (stages <- pipelineModel.stages) {
      stages match {
        case i: StringIndexerModel =>
          val stageModel = i
          if (stageModel.getOutputCol == labelColName) {
            labelValues = stageModel.labels
            isCategoricalLabel = true
          }
        case _ =>
      }
    }
    if (isCategoricalLabel) {
      for (classIndex <- 0 until numClasses) {
        labelTransformationDetail.put(classIndex.toDouble, labelValues(classIndex).toString)
      }
    }
    else {
      for (classIndex <- 0 until numClasses) {
        labelTransformationDetail.put(classIndex.toDouble, classIndex.toString)
      }
    }
    Some(labelTransformationDetail.toMap)
  }
}
