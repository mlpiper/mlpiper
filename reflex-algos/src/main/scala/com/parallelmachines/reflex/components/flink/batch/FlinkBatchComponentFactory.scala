package com.parallelmachines.reflex.components.flink.batch

import com.parallelmachines.reflex.components.flink
import com.parallelmachines.reflex.factory.ByClassComponentFactory
import com.parallelmachines.reflex.pipeline.ComputeEngineType

import scala.collection.mutable


/**
  * A factory specific to Flink batch components
  * @param classList List of class names to register
  */
class FlinkBatchComponentFactory(classList: mutable.MutableList[Class[_]])
  extends ByClassComponentFactory(ComputeEngineType.FlinkBatch, classList: mutable.MutableList[Class[_]])


object FlinkBatchComponentFactory {
  private val flinkBatchComponents = mutable.MutableList[Class[_]](
    // Input
    classOf[flink.batch.connectors.FlinkBatchFileConnector],
    classOf[flink.batch.connectors.FlinkBatchHDFSConnector],
    // Parsing
    classOf[flink.batch.parsers.ReflexStringToBreezeVectorBatch],
    classOf[flink.batch.parsers.ReflexStringToLabeledVectorBatch],
    // Output
    classOf[flink.batch.connectors.FlinkBatchMLHealthModelFileSink],
    classOf[flink.batch.connectors.FlinkBatchStdoutConnector],
    classOf[flink.batch.connectors.EventSocketSink],
    classOf[flink.batch.connectors.FlinkBatchNullConnector],
    classOf[flink.batch.connectors.ReflexS3SinkBatchComponent],

    // Flow control
    classOf[flink.batch.general.TwoDup]
  )

  def apply(testMode: Boolean): FlinkBatchComponentFactory = new FlinkBatchComponentFactory(flinkBatchComponents)
}
