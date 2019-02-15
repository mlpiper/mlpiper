package com.parallelmachines.reflex.components.spark.batch.algorithms

/**
  * This component runs feature engineering routines and inference
  * described by a given sparkML model over the Spark engine.
  */

import java.io.{ByteArrayInputStream, File}
import java.nio.file.Files

import com.parallelmachines.reflex.common.mlobject.Model
import com.parallelmachines.reflex.common.{ExtractArchives, _}
import com.parallelmachines.reflex.components.flink.streaming.algorithms.{ModelBehavior, ModelBehaviorType}
import com.parallelmachines.reflex.components.spark.batch.connectors.{EventSocketSource, ModelTypeString, ReflexNullConnector, RestDataSource}
import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.{ComponentAttribute, EnablePerformanceComponentAttribute, EnableValidationComponentAttribute, LabelColComponentAttribute}
import com.parallelmachines.reflex.pipeline._
import org.apache.commons.io.FileUtils
import org.apache.flink.streaming.scala.examples.clustering.stat.heatmap.HeatMap
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexSparkMLInferenceComponent extends SparkBatchComponent with ModelBehavior {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.algorithms
  override val label: String = "sparkML Inference for DataFrames"
  override val description: String = "Run sparkML Feature Engineering and Inference routines" +
    " on input dataset."
  override val version: String = "1.0.0"


  // TODO: enable health stats [REF-1086]
  val input1 = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "Dataframe",
    description = "Dataframe of attribute",
    group = ConnectionGroups.DATA)

  val input2 = ComponentConnection(
    tag = typeTag[Model],
    defaultComponentClass = Some(classOf[RestDataSource]),
    eventTypeInfo = Some(EventDescs.Model),
    label = "Model",
    description = "Model used to serve predictions",
    group = ConnectionGroups.MODEL)

  val input3 = ComponentConnection(
    tag = typeTag[RDD[String]],
    defaultComponentClass = Some(classOf[RestDataSource]),
    eventTypeInfo = Some(EventDescs.MLHealthModel),
    label = "Contender Health Stat",
    description = "ML Health Stream",
    group = ConnectionGroups.STATISTICS)

  val output = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    defaultComponentClass = Some(classOf[ReflexNullConnector]),
    label = "Prediction DataFrame",
    description = "Prediction output",
    group = ConnectionGroups.PREDICTION)

  override val inputTypes: ConnectionList = ConnectionList(input1, input2, input3)
  override var outputTypes: ConnectionList = ConnectionList(output)


  override val modelBehaviorType: ModelBehaviorType.Value = ModelBehaviorType.ModelConsumer

  /* TODO: figure out if performanceMetrics and validation are needed here */
  val performanceMetrics = EnablePerformanceComponentAttribute()
  val validation = EnableValidationComponentAttribute()
  val labelField = LabelColComponentAttribute()
  val tempSharedPath = ComponentAttribute("tempSharedPath", "",
    "temp Shared Path", "Temporary shared path for model transfer, " +
      "paths with prefix file:// or hdfs://", optional = true)

  private var enableHealth = true

  attrPack.add(performanceMetrics, validation, labelField, tempSharedPath)


  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)

    enableHealth = paramMap.getOrElse(ReflexSystemConfig.EnableHealthKey, true).asInstanceOf[Boolean]
    ModelTypeString.setModelTypeString(false)
  }

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {
    ModelTypeString.setModelTypeString(false)
    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val model = dsArr(1).data[Model]()
    val originalDataframe = pipelineInfo.dataframe

    val transformedDataframe = pipelineInfo.fit().transform(originalDataframe)

    val sparkSession: SparkSession =
      SparkSession.builder
        .master(env.getConf.get("spark.master"))
        .getOrCreate
    var sparkMLModel: PipelineModel = null
    var tempPath = ""
    try {
      val modelData = new ByteArrayInputStream(model.getData.get)
      val tempDir = Files.createTempDirectory("tempA")
      tempPath = tempDir.toString
      val mainPath = ExtractArchives.extractTarGZipObj(tempPath, modelData)

      val sparkMLPipelineModelHelper = new SparkMLPipelineModelHelper()
      sparkMLPipelineModelHelper.setSharedContext(sparkContext1 = env)
      sparkMLPipelineModelHelper.setLocalPath(tempPath + "/" + mainPath)
      sparkMLPipelineModelHelper.setSharedPathPrefix(tempSharedPath.value)

      sparkMLModel = sparkMLPipelineModelHelper.loadSparkmlModel()

    }
    catch {
      case e: Exception => logger.error(s"exception caught: $e")
        val emptyRDD = env.emptyRDD[Row]
        val schema = new StructType().add("InferenceFailed", "string")
        val emptyDF = sparkSession.createDataFrame(emptyRDD, schema)
        return ArrayBuffer(DataWrapper(new SparkBatchPipelineInfo(emptyDF)))
    }

    val healthLib = new HealthLibSpark(enableHealth)
    healthLib.setContext(env)
    healthLib.setDfOfDenseVector(transformedDataframe)
    healthLib.setIncomingHealth(dsArr(2).data[RDD[String]]())

    val continuousHealthStat = new ContinuousHistogramForSpark(HealthType.ContinuousHistogramHealth.toString)
    continuousHealthStat.setModelId(model.getId)

    healthLib.addComponent(continuousHealthStat)

    val categoricalHealthStat = new CategoricalHistogramForSpark(HealthType.CategoricalHistogramHealth.toString)
    categoricalHealthStat.setModelId(model.getId)

    healthLib.addComponent(categoricalHealthStat)

    healthLib.generateHealth()

    val rddOfNamedVec = DataFrameUtils.toRDDOfNamedVectorUsingSparkML(df = transformedDataframe, sparkMLModel = Option(sparkMLModel), columnMap = None)

    val heatMapValues = HeatMap.createHeatMap(rddOfNamedVec = rddOfNamedVec, env = env)

    val predictionDF = sparkMLModel.transform(transformedDataframe)
    pipelineInfo.setTransformedDataframe(predictionDF)
    // TODO: handle of skip elements is worse than jpmml. SPark 2.3 fixes part of it.

    val predictionStatsForSpark = new SparkMLPredictionStatsForSpark()

    predictionStatsForSpark.setPredictionDF(predictionDF)
    val predictionDFLabeled = predictionStatsForSpark.createStat(
      model = sparkMLModel,
      sc = env)

    val orgColNames = transformedDataframe.columns
    val predictionColName = predictionStatsForSpark.getPredictionColName()
    val predColNames = orgColNames :+ predictionColName
    if (tempPath != "") {
      FileUtils.deleteDirectory(new File(tempPath))
    }
    ArrayBuffer(DataWrapper(new SparkBatchPipelineInfo(predictionDFLabeled.select(predColNames.head, predColNames.tail: _*))))
  }
}
