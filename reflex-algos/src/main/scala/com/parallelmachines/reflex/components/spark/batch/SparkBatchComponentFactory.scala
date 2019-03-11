package com.parallelmachines.reflex.components.spark.batch

import com.parallelmachines.reflex.components.spark
import org.mlpiper.infrastructure.ComputeEngineType
import org.mlpiper.infrastructure.factory.ByClassComponentFactory

import scala.collection.mutable


/**
  * A SparkBactch components factory
  *
  * @param classList List of class names to register
  */
class SparkBatchComponentFactory(classList: mutable.MutableList[Class[_]])
  extends ByClassComponentFactory(ComputeEngineType.SparkBatch, classList: mutable.MutableList[Class[_]])


object SparkBatchComponentFactory {

  private val sparkBatchComponents = mutable.MutableList[Class[_]](
    // Algorithms
    classOf[spark.batch.algorithms.ReflexKmeansML],
    classOf[spark.batch.algorithms.ReflexRandomForestML],
    classOf[spark.batch.algorithms.ReflexLogisticRegression],
    classOf[spark.batch.algorithms.ReflexGBTRegML],
    classOf[spark.batch.algorithms.ReflexGBTML],
    classOf[spark.batch.algorithms.ReflexGLM],
    classOf[spark.batch.algorithms.ReflexLinearRegression],
    classOf[spark.batch.algorithms.ReflexRandomForestRegML],
    classOf[spark.batch.algorithms.ReflexSparkMLInferenceComponent],
    classOf[spark.batch.algorithms.ReflexDTML],
    classOf[spark.batch.algorithms.ReflexDTRegML],

    // Connectors
    classOf[spark.batch.connectors.FromStringCollection],
    classOf[spark.batch.connectors.FromTextFile],
    classOf[spark.batch.connectors.SaveToFile],
    classOf[spark.batch.connectors.SparkModelFileSink],
    classOf[spark.batch.connectors.ReflexNullConnector],
    classOf[spark.batch.connectors.ReflexNullSourceConnector],
    classOf[spark.batch.connectors.EventSocketSource],
    classOf[spark.batch.connectors.EventSocketSink],
    classOf[spark.batch.connectors.RestDataSource],
    classOf[spark.batch.parsers.CsvToDF],
    classOf[spark.batch.connectors.DFtoFile],
    classOf[spark.batch.connectors.HiveToDF],
    classOf[spark.batch.parsers.SnowFlakeToDF],
    classOf[spark.batch.connectors.DFToSnowFlake],
    classOf[spark.batch.connectors.DFtoHive],
    classOf[spark.batch.connectors.MySQLToDF],
    classOf[spark.batch.connectors.DFtoMySQL],
    classOf[spark.batch.connectors.DFSplit],

    // FE
    classOf[spark.batch.fe.MaxAbsoluteScalerPipeline],
    classOf[spark.batch.fe.NormalizerPipeline],
    classOf[spark.batch.fe.MinMaxScalerPipeline],
    classOf[spark.batch.fe.VectorAssemblerComponent],
    classOf[spark.batch.fe.PCAFE],
    classOf[spark.batch.fe.VectorIndexerComponent],
    classOf[spark.batch.fe.StringIndexerComponent],
    classOf[spark.batch.fe.BinarizerComponent],
    classOf[spark.batch.fe.BucketizerComponent],
    classOf[spark.batch.fe.VectorSlicerComponent],
    classOf[spark.batch.fe.QuantileDiscretizerComponent],
    classOf[spark.batch.fe.OneHotEncoderComponent],
    classOf[spark.batch.fe.RFormulaComponent],
    classOf[spark.batch.fe.ChiSqSelectorComponent],
    classOf[spark.batch.fe.IndexToStringComponent],
    classOf[spark.batch.fe.StandardScalerPipeline],

    // General
    classOf[spark.batch.general.TwoDup],

    // Stat
    classOf[spark.batch.stat.HistogramProducerSpark]
  )

  private val testSparkBatchComponents = mutable.MutableList[Class[_]](
    classOf[spark.batch.dummy.TestLongLastingOperation],
    classOf[spark.batch.dummy.TestStdoutSink],
    classOf[spark.batch.dummy.TestAccumulators],
    classOf[spark.batch.connectors.SparkCollectConnector],
    classOf[spark.batch.dummy.TestSparkBatchAlgoComponent],
    classOf[spark.batch.dummy.TestHealthComponent]
  )

  def apply(testMode: Boolean = false): SparkBatchComponentFactory = {
    if (!testMode) {
      new SparkBatchComponentFactory(sparkBatchComponents)
    } else {
      val l = sparkBatchComponents ++ testSparkBatchComponents
      new SparkBatchComponentFactory(l)
    }
  }
}
