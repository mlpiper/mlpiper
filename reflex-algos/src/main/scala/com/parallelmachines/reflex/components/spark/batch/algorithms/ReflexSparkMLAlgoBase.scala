package com.parallelmachines.reflex.components.spark.batch.algorithms

import java.io.File
import java.nio.file.Files

import com.parallelmachines.reflex.common._
import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.spark.batch.algorithms.MlMethod.MlMethodType
import com.parallelmachines.reflex.components.spark.batch.connectors.{ReflexNullConnector, ReflexNullSourceConnector}
import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.ml.{PipelineModel, PipelineStage}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.mlpiper.datastructures.SparkMLModel
import org.mlpiper.infrastructure._
import org.mlpiper.infrastructure.rest.RestApis
import org.mlpiper.sparkutils.{SparkMLFeatureDetails, SparkMLLabelDetails, SparkMLPipelineModelHelper}
import org.mlpiper.stat.algos.{ClassificationEvaluation, RegressionEvaluation}
import org.mlpiper.stat.fi.FeatureImportance
import org.mlpiper.stat.healthlib.{CategoricalHealthForSpark, ContinuousHistogramForSpark, HealthLibSpark, HealthType}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

trait ReflexSparkMLAlgoBase extends SparkBatchComponent with ModelBehavior {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.algorithms
  lazy val defaultModelName: String = ""

  private val train = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataframeTrain",
    description = "Data frame of labels and features",
    group = ConnectionGroups.DATA)

  private val validate = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    defaultComponentClass = Some(classOf[ReflexNullSourceConnector]),
    label = "DataframeValidate",
    description = "Data frame of labels and features to validate",
    group = ConnectionGroups.DATA)

  private val outputModel = ComponentConnection(
    tag = typeTag[SparkMLModel],
    defaultComponentClass = Some(classOf[ReflexNullConnector]),
    label = "Model",
    description = "Trained model",
    group = ConnectionGroups.MODEL)

  private val outputTrDF = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    defaultComponentClass = Some(classOf[ReflexNullConnector]),
    label = "DataFramePredicted",
    description = "Dataframe with Predictions",
    group = ConnectionGroups.PREDICTION)

  private var enableHealth = true

  override val inputTypes: ConnectionList = ConnectionList(train, validate)
  override var outputTypes: ConnectionList = ConnectionList(outputModel, outputTrDF)

  override val modelBehaviorType: ModelBehaviorType.Value = ModelBehaviorType.ModelProducer

  var featuresColName: String = _
  var supportFeatureImportance = false
  var significantFeatures: Int = 0

  var tempSharedPathStr: String = _

  val mlType: MlMethodType = MlMethod.Classification

  val modelName = ComponentAttribute("modelName", s"${this.defaultModelName}", "Model Name", "Model name to be displayed.", optional = true)
  attrPack.add(modelName)

  def getAlgoStage: PipelineStage

  /** Method provides abstract definition for generating training stats.
    * Algorithm should override this method to generate stats.
    * Default behavior of this method is to Do Nothing.
    */
  def generateModelStat(pipelineModel: PipelineModel,
                        transformed_df: DataFrame,
                        sparkContext: SparkContext): Unit = {
    /*Do Nothing*/
  }

  /** Method should be overridden and return label column name for algorithms that use labeled dataset  */
  def getLabelColumnName: Option[String] = None

  final def generalAlgoEvaluator(env: SparkContext,
                                 dsArr: ArrayBuffer[DataWrapperBase],
                                 pipelineModel: PipelineModel)
  : Unit = {
    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val validation = dsArr(1).data[SparkBatchPipelineInfo]()
    val train = dsArr(0).data[SparkBatchPipelineInfo]()

    val validationDf =
    // if validation df is empty then we can use training df as validator
      if (validation.dataframe.head(1).isEmpty) {
        logger.debug("Using training dataset as validation dataset")
        train.dataframe
      }
      else {
        logger.debug("Using provided validation dataset as validation dataset")
        validation.dataframe
      }

    val predictedDF = pipelineModel.transform(validationDf)
    val predictedLabelColumn = predictedDF.schema.fieldNames.last //TODO: change it to get the prediction column name

    // set the transformed dataframe
    pipelineInfo.setTransformedDataframe(predictedDF)

    if (supportFeatureImportance & (significantFeatures > 0)) {
      FeatureImportance.exportFeatureImportance(env, predictedDF, pipelineModel, significantFeatures)
    }
    val sparkMLFeatureDetails = new SparkMLFeatureDetails()
    val featureDetails = sparkMLFeatureDetails.getFeatureCategory(sparkMLModel = pipelineModel, dfSchema = predictedDF.schema)
    logger.error(s"=======> featureDetails:\n $featureDetails")

    if (getLabelColumnName.isDefined) {
      val validationLabelColumn = getLabelColumnName.get

      val mergedPredictedAndActualLabelDFs: DataFrame =
        predictedDF.select(predictedLabelColumn, validationLabelColumn)

      mlType match {
        case MlMethod.Classification =>
          val labelTransformationDetails =
            SparkMLLabelDetails.getLabelTransformationDetails(pipelineModel = pipelineModel)

          LoggerFactory.getLogger(getClass).debug(s"Label Transformation Details Are: $labelTransformationDetails")

          val evaluationStatistics = ClassificationEvaluation
            .generateStats(
              dataFrame = mergedPredictedAndActualLabelDFs,
              labelTransformationDetails = labelTransformationDetails
            )

          val classificationEvaluationObject: ClassificationEvaluation =
            new ClassificationEvaluation(
              evaluationStatistics = evaluationStatistics,
              sparkContext = env
            )

          classificationEvaluationObject.updateStatAccumulator()

        case MlMethod.Regression =>
          val evaluationRegStatistics = RegressionEvaluation
            .generateStats(
              dataFrame = mergedPredictedAndActualLabelDFs
            )

          val regressionEvaluationObject: RegressionEvaluation =
            new RegressionEvaluation(evaluationRegStatistics = evaluationRegStatistics,
              sparkContext = env)

          regressionEvaluationObject.updateStatAccumulator()
      }
    }
  }

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)
    enableHealth = paramMap.getOrElse(ReflexSystemConfig.EnableHealthKey, true).asInstanceOf[Boolean]
  }

  final override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                                 errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    var pipelineModel: PipelineModel = null
    var schema: StructType = null

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()

    val algoStage = getAlgoStage

    require(featuresColName != null, s"featuresColName must be set in classes which inherit from this base ${getClass.getSimpleName}")
    pipelineInfo.addStage(stage = algoStage)

    pipelineModel = pipelineInfo.fit()

    schema = pipelineInfo.dataframe.schema

    val modelDesc = s"Generated by ${this.defaultModelName} internal component"
    val m = new SparkMLModel(modelName.value, modelDesc)

    generalAlgoEvaluator(env, dsArr, pipelineModel)
    // NOTE: transformed dataframe must be set at this point
    assert(pipelineInfo.getTransformedDataframe().isDefined, "Transformed dataframe is not set")

    //calculate histogram for training data
    val healthLib = new HealthLibSpark(enableHealth)
    healthLib.setContext(env)
    healthLib.setDfOfDenseVector(pipelineInfo.dataframe)

    // continuous histogram calculation
    val continuousHistogramForSpark = new ContinuousHistogramForSpark(HealthType.ContinuousHistogramHealth.toString)

    continuousHistogramForSpark.enableAccumOutputOfHistograms = true
    continuousHistogramForSpark.sparkMLModel = Option(pipelineModel)
    continuousHistogramForSpark.setModelId(m.getId)

    healthLib.addComponent(continuousHistogramForSpark)

    // categorical histogram calculation
    val categoricalHistogramForSpark = new CategoricalHealthForSpark(HealthType.CategoricalHistogramHealth.toString)

    categoricalHistogramForSpark.enableAccumOutputOfHistograms = true
    categoricalHistogramForSpark.sparkMLModel = Option(pipelineModel)
    categoricalHistogramForSpark.setModelId(m.getId)

    healthLib.addComponent(categoricalHistogramForSpark)

    healthLib.generateHealth()

    // generating model related stat
    // NOTE: asserted for non null already
    val transformedDF = pipelineInfo.getTransformedDataframe().get

    generateModelStat(pipelineModel = pipelineModel, transformed_df = transformedDF, sparkContext = env)


    m.setSparkMLModel(pipelineModel)
    m.setTempSharedPathStr(tempSharedPathStr)

    /**
      * Create temp directory and copy model from shared storage to local.
      **/
    val tmpModelDir = Files.createTempDirectory("tmpModelDir").toString
    val sparkMLPipelineModelHelper = new SparkMLPipelineModelHelper()
    sparkMLPipelineModelHelper.setSharedContext(sparkContext1 = env)
    sparkMLPipelineModelHelper.setLocalPath(tmpModelDir)
    sparkMLPipelineModelHelper.setSharedPathPrefix(tempSharedPathStr)
    sparkMLPipelineModelHelper.saveSparkmlModel(pipelineModel)

    /**
      * Archive directory with the model and set bytes as a model data.
      **/
    val modelArchiveBytes = new DirectoryPack().pack(tmpModelDir)
    m.setData(modelArchiveBytes)

    RestApis.publishModel(m)

    if (tmpModelDir != "") {
      FileUtils.deleteDirectory(new File(tmpModelDir))
    }

    ArrayBuffer(DataWrapper(m),
      DataWrapper(new SparkBatchPipelineInfo(transformedDF)))
  }
}

/** Object represents type of ML algorithm */
object MlMethod extends Enumeration {
  type MlMethodType = Value
  val Classification: Value = Value("classification")
  val Regression: Value = Value("regression")

  def contains(mlMethod: String): Boolean = values.exists(_.toString == mlMethod)

  override def toString: String = s"Supported ML Methods: ${values.mkString(", ")}"
}
