package org.mlpiper.mlops

import com.parallelmachines.reflex.common.InfoType.InfoType
import com.parallelmachines.reflex.common._
import com.parallelmachines.reflex.pipeline.DataFrameUtils
import com.parallelmachines.reflex.pipeline.spark.stats.SystemStatsListener
import org.apache.flink.streaming.scala.examples.common.stats.{AccumData, _}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.mlpiper.stat.healthlib.{CategoricalHealthForSpark, ContinuousHistogramForSpark, HealthLibSpark, HealthType}
import org.mlpiper.stat.heatmap.continuous.HeatMap
import org.slf4j.LoggerFactory

import scala.collection.mutable

case class JSONData(jsonData: String) {
  override def toString: String = {
    jsonData
  }
}

object MLOps {

  // TODO: find which prefix to use if any
  val accPrefix = "mlops."
  var sparkContext: JavaSparkContext = _
  var sparkSession: SparkSession = _
  var waitForExit: Boolean = false // TODO: get waitForExit via configuration
  private var initCalled: Boolean = false

  // TODO: find how to see what going out via logs.info
  private val logger = LoggerFactory.getLogger(getClass)

  // This map will contain a mapping from a name to the statistics item (accumulator) to use
  private val accMap: mutable.Map[String, GlobalAccumulator[Any]] = mutable.Map[String, GlobalAccumulator[Any]]()

  /**
    * To be used by the python mlops to check the object is loaded and valid
    */
  def ping(v: Int): Int = {
    v
  }

  def init(sc: JavaSparkContext, startRest: Boolean = true, restServerPort: Int, waitForExitVal: Boolean): Unit = {

    if (initCalled) {
      logger.warn("Init was already called - avoiding re-init")
      return
    }

    logger.info("MLOps module init - setting spark context")
    sparkContext = sc
    waitForExit = waitForExitVal

    logger.info(s"Starting REST server on port $restServerPort")

    logger.info("Adding new spark listener")
    sparkContext.addSparkListener(new SystemStatsListener())

    sparkSession = SparkSession.builder().master(sparkContext.sc.master).getOrCreate()
  }

  def done(): Unit = {
    logger.info("Done with mlops library...")
    logger.info("Reflex app exited gracefully!")
  }

  private def statWithType[T](name: String,
                              value: T,
                              dataType: AccumData.GraphType,
                              modeType: AccumMode.AccumModeType,
                              modelId: String,
                              infoType: InfoType = InfoType.General,
                              timestamp_ns: String = null): Unit = {
    logger.info(s"stat-with-type name: $name")

    var acc: GlobalAccumulator[T] = null
    if (accMap.contains(name)) {
      logger.info("Stat already exists")
      acc = accMap(name).asInstanceOf[GlobalAccumulator[T]]
      acc.localUpdate(value, timestamp_ns)
    } else {
      acc = StatInfo(name).toGlobalStat(value, dataType, modeType, infoType, modelId, timestamp_ns)
      logger.info("First time this stats is reported")
      accMap(name) = acc.asInstanceOf[GlobalAccumulator[Any]]
    }

    acc.updateSparkAccumulator(sparkContext)

  }

  def stat(name: String, value: Double, modelId: String, graphType: String, statMode: String): Unit = {
    statWithType[Double](name, value, AccumData.withName(graphType), AccumMode.withName(statMode), modelId)
  }

  def stat(name: String, value: Int, modelId: String, graphType: String, statMode: String): Unit = {
    statWithType[Int](name, value, AccumData.withName(graphType), AccumMode.withName(statMode), modelId)
  }

  def stat(name: String, value: Long, modelId: String, graphType: String, statMode: String): Unit = {
    statWithType[Long](name, value, AccumData.withName(graphType), AccumMode.withName(statMode), modelId)
  }

  def stat(name: String, value: String, modelId: String, graphType: String, statMode: String): Unit = {
    statWithType[String](name, value, AccumData.withName(graphType), AccumMode.withName(statMode), modelId)
  }

  def statTable(name: String, value: String, modelId: String): Unit = {
    logger.info("statTable: " + value)
    val tblData = JSONData(value)
    statWithType[JSONData](name, tblData, AccumData.Matrix, AccumMode.Instant, modelId)
  }

  def statJSON(name: String, jsonStr: String, modelId: String, graphType: String, statMode: String,
               infoType: Int = InfoType.General.value, timestamp_ns: String = null): Unit = {
    val jsonStrData = JSONData(jsonStr)
    statWithType[JSONData](name, jsonStrData, AccumData.withName(graphType), AccumMode.withName(statMode), modelId,
      InfoType.fromValue(infoType), timestamp_ns)
  }

  def stat(name: String, value: java.util.List[Double], modelId: String, graphType: String, statMode: String): Unit = {
    statWithType[java.util.List[Double]](name, value, AccumData.withName(graphType), AccumMode.withName(statMode), modelId)
  }

  def stat(name: String, value: java.util.HashMap[String, _], modelId: String, graphType: String, statMode: String): Unit = {
    statWithType[java.util.HashMap[String, _]](name, value, AccumData.withName(graphType), AccumMode.withName(statMode), modelId)
  }

  def inputStatsFromRDD(name: String,
                        modelId: String,
                        rdd: JavaRDD[org.apache.spark.mllib.linalg.DenseVector],
                        histRDD: JavaRDD[String])
  : Unit = {
    logger.debug(s"calculating input statistics on rdd[vector] name: $name\n")

    // Create a schema for the dataframe
    var schema = new StructType()
    val numCols = rdd.rdd.map(x => x.toArray).take(1)(0).length
    for (i <- 0 until numCols) schema = schema.add("c%d".format(i), "double")
    val rddRow = rdd.rdd.map(x => Row.fromSeq(x.toArray))
    val df = sparkSession.createDataFrame(rddRow, schema)

    val healthLib = new HealthLibSpark(true)
    healthLib.setContext(sparkContext)
    healthLib.setDfOfDenseVector(df)

    if (histRDD.count() != 0) {
      healthLib.setIncomingHealth(histRDD.rdd)
    }

    val histStat = new ContinuousHistogramForSpark(HealthType.ContinuousHistogramHealth.toString)
    histStat.setModelId(modelId)
    histStat.sparkMLModel = None
    healthLib.addComponent(histStat)

    // categorical histogram calculation
    val categoricalHistogramForSpark = new CategoricalHealthForSpark(HealthType.CategoricalHistogramHealth.toString)

    categoricalHistogramForSpark.enableAccumOutputOfHistograms = true
    categoricalHistogramForSpark.setModelId(modelId)

    healthLib.addComponent(categoricalHistogramForSpark)

    healthLib.generateHealth()

    val rddOfNamedVec = DataFrameUtils.toRDDOfNamedVectorUsingSparkML(df = df, sparkMLModel = None, columnMap = None)

    val heatMapValues = HeatMap.createHeatMap(rddOfNamedVec = rddOfNamedVec, env = df.sqlContext.sparkContext)
  }

  def inputStatsFromDataFrame(name: String,
                              modelId: String,
                              df: DataFrame,
                              histRDD: JavaRDD[String]): Unit = {
    inputStatsFromDataFrame(name, modelId, df, histRDD, null)
  }

  def inputStatsFromDataFrame(name: String,
                              modelId: String,
                              df: DataFrame,
                              histRDD: JavaRDD[String],
                              sparkMLModel: PipelineModel)
  : Unit = {
    logger.info("Calculating Health And DA Using Spark ML Model! " + Option(sparkMLModel))
    val healthLib = new HealthLibSpark(true)
    healthLib.setContext(sparkContext)
    healthLib.setDfOfDenseVector(df)
    if (histRDD.count() != 0) {
      healthLib.setIncomingHealth(histRDD.rdd)
    }

    val histStat = new ContinuousHistogramForSpark(HealthType.ContinuousHistogramHealth.toString)
    histStat.enableAccumOutputOfHistograms = true
    histStat.sparkMLModel = Option(sparkMLModel)
    histStat.setModelId(modelId)

    healthLib.addComponent(histStat)

    // categorical histogram calculation
    val categoricalHistogramForSpark = new CategoricalHealthForSpark(HealthType.CategoricalHistogramHealth.toString)

    categoricalHistogramForSpark.enableAccumOutputOfHistograms = true
    categoricalHistogramForSpark.sparkMLModel = Option(sparkMLModel)
    categoricalHistogramForSpark.setModelId(modelId)

    healthLib.addComponent(categoricalHistogramForSpark)

    healthLib.generateHealth()

    val rddOfNamedVec =
      DataFrameUtils.toRDDOfNamedVectorUsingSparkML(df = df, sparkMLModel = Option(sparkMLModel), columnMap = None)

    val heatMapValues = HeatMap.createHeatMap(rddOfNamedVec = rddOfNamedVec, env = df.sqlContext.sparkContext)
  }
}
