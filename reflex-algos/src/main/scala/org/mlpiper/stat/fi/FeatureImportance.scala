package org.mlpiper.stat.fi

import com.parallelmachines.reflex.common.InfoType
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, GBTClassificationModel, RandomForestClassificationModel}
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, GBTRegressionModel, RandomForestRegressionModel}
import org.apache.spark.sql.DataFrame
import org.mlpiper.stats._
import org.mlpiper.utils.ParsingUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable


/**
  * Object [[FeatureImportance]] is responsible to provide feature Importance for supported models
  */
object FeatureImportance {
  private val logger = LoggerFactory.getLogger(getClass)

  def exportFeatureImportance(env: SparkContext,
                              df: DataFrame,
                              pipelineModel: PipelineModel,
                              significantFeatures: Int
                             ): Unit = {

    val pipelineStage = pipelineModel.stages(pipelineModel.stages.length - 1)
    var featureImportanceVector = Array[Double]()
    var featuresColName = ""
    pipelineStage match {
      case i: RandomForestClassificationModel =>
        val stageModel = i
        featureImportanceVector = stageModel.featureImportances.toArray
        featuresColName = stageModel.getFeaturesCol
      case i: RandomForestRegressionModel =>
        val stageModel = i
        featureImportanceVector = stageModel.featureImportances.toArray
        featuresColName = stageModel.getFeaturesCol
      case i: GBTClassificationModel =>
        val stageModel = i
        featureImportanceVector = stageModel.featureImportances.toArray
        featuresColName = stageModel.getFeaturesCol
      case i: GBTRegressionModel =>
        val stageModel = i
        featureImportanceVector = stageModel.featureImportances.toArray
        featuresColName = stageModel.getFeaturesCol
      case i: DecisionTreeClassificationModel =>
        val stageModel = i
        featureImportanceVector = stageModel.featureImportances.toArray
        featuresColName = stageModel.getFeaturesCol
      case i: DecisionTreeRegressionModel =>
        val stageModel = i
        featureImportanceVector = stageModel.featureImportances.toArray
        featuresColName = stageModel.getFeaturesCol
      case _ =>
    }

    val struct_fieldIndex = df.schema.fieldIndex(featuresColName)
    val struct_field = df.schema.fields(struct_fieldIndex)
    val attrStruct = struct_field.metadata.getMetadata("ml_attr").getMetadata("attrs")
    val featureImportanceNames = new Array[String](featureImportanceVector.length)

    logger.debug("Attribute structure of algorithm feature vector" + attrStruct)
    var total_names = 0

    if (attrStruct.contains("numeric")) {
      for (attrList <- attrStruct.getMetadataArray("numeric")) {
        featureImportanceNames(attrList.getLong("idx").toInt) = attrList.getString("name")
        total_names += 1
      }
    }
    if (attrStruct.contains("nominal")) {
      for (attrList <- attrStruct.getMetadataArray("nominal")) {
        featureImportanceNames(attrList.getLong("idx").toInt) = attrList.getString("name")
        total_names += 1
      }
    }

    if (attrStruct.contains("binary")) {
      for (attrList <- attrStruct.getMetadataArray("binary")) {
        featureImportanceNames(attrList.getLong("idx").toInt) = attrList.getString("name")
        total_names += 1
      }
    }
    if (total_names == featureImportanceVector.length) {


      val (featureImportanceVectorSorted, featureImportanceVectorSortedIndices) =
        featureImportanceVector.zipWithIndex.sorted.reverse.unzip
      val featureImportanceNamesSorted =
        featureImportanceVectorSortedIndices.map(v => featureImportanceNames(v))

      logger.debug("Important_named_features_sorted = " + featureImportanceNamesSorted.mkString(","))
      logger.debug("Important_values_features_sorted = " + featureImportanceVectorSorted.mkString(","))

      var takSignificantFeatures = featureImportanceVectorSortedIndices.length
      if (significantFeatures < takSignificantFeatures) {
        takSignificantFeatures = significantFeatures
      }
      val featureImportanceVectorSortedIndicesLimit = featureImportanceVectorSortedIndices.take(takSignificantFeatures)

      val mapOfFeatureImportance: mutable.LinkedHashMap[String, Double] = mutable.LinkedHashMap[String, Double]()

      featureImportanceVectorSortedIndicesLimit.foreach(featureImportanceIndex => {
        mapOfFeatureImportance.put(featureImportanceNames(featureImportanceIndex), featureImportanceVector(featureImportanceIndex))
      })


      logger.info("Important_features_sorted = " + mapOfFeatureImportance.mkString(","))

      FeatureImportanceStat.createStat(env, mapOfFeatureImportance)
    }
    else {

      throw new RuntimeException("feature names length not fitting the importance vector. Got: "
        + total_names + " expected: " + featureImportanceVector.length)
    }
  }
}


/**
  * Object is responsible for providing a bargraph for feature Importance.
  */
object FeatureImportanceStat extends Serializable {

  /** FeatureImportance Wrapper holds map of feature name and importance value . */
  case class FeatureImportanceWrapper(featureImportance: mutable.LinkedHashMap[String, Double]) extends GraphFormatOrdered {
    override def toString: String = {
      val mapOfFeatureImportanceStat = Map[String, Any]("features" -> this.toGraphJsonable)
      ParsingUtils.iterableToJSON(mapOfFeatureImportanceStat)
    }

    override def getDataMap: mutable.LinkedHashMap[String, Double] = this.featureImportance
  }

  private class AccumulatorUpdater(sc: SparkContext) {

    /** Method is responsible for providing accumulator for feature Importance  */
    private def getFeatureImportanceAccumulator(featureImportanceWrapper: FeatureImportanceWrapper)
    : GlobalAccumulator[FeatureImportanceWrapper] = {
      new GlobalAccumulator[FeatureImportanceWrapper](
        name = StatNames.featureImportanceStat(),
        // globally, AllCategoryProbabilityStatWrapper will be replaced
        localMerge = (_: AccumulatorInfo[FeatureImportanceWrapper],
                      newFeatureImportanceWrapper: AccumulatorInfo[FeatureImportanceWrapper]) => {
          newFeatureImportanceWrapper
        },
        globalMerge = (_: AccumulatorInfo[FeatureImportanceWrapper],
                       newFeatureImportanceWrapper: AccumulatorInfo[FeatureImportanceWrapper]) => {
          newFeatureImportanceWrapper
        },
        startingValue = featureImportanceWrapper,
        accumDataType = AccumData.BarGraph,
        accumModeType = AccumMode.Instant,
        infoType = InfoType.InfoType.General
      )
    }

    private var globalStatForFeatureImportance: GlobalAccumulator[FeatureImportanceWrapper] = _

    /** Method will update All Categories Probability Stat in associated accumulator */
    def updateFeatureImportanceAccumulator(featureImportanceWrapper: FeatureImportanceWrapper): Unit = {
      if (globalStatForFeatureImportance != null) {
        globalStatForFeatureImportance.localUpdate(featureImportanceWrapper)
      } else {
        globalStatForFeatureImportance = getFeatureImportanceAccumulator(featureImportanceWrapper)
      }

      globalStatForFeatureImportance.updateSparkAccumulator(sc)
    }
  }


  def createStat(sc: SparkContext, mapOfFeatureImportance: mutable.LinkedHashMap[String, Double]): Unit = {

    val featureImportanceWrapper = FeatureImportanceWrapper(featureImportance = mapOfFeatureImportance)

    new AccumulatorUpdater(sc = sc)
      .updateFeatureImportanceAccumulator(featureImportanceWrapper = featureImportanceWrapper)
  }
}
