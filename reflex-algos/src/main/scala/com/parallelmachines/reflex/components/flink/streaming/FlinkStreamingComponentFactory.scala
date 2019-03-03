package com.parallelmachines.reflex.components.flink.streaming

import com.parallelmachines.reflex.components.flink
import com.parallelmachines.reflex.factory.ByClassComponentFactory
import com.parallelmachines.reflex.pipeline.ComputeEngineType

import scala.collection.mutable


/**
  * A factory for FlinkStreaming components
  *
  * @param classList List of class names to register
  */
class FlinkStreamingComponentFactory(classList: mutable.MutableList[Class[_]])
  extends ByClassComponentFactory(ComputeEngineType.FlinkStreaming, classList: mutable.MutableList[Class[_]])


object FlinkStreamingComponentFactory {
  private val flinkStreamingComponents = mutable.MutableList[Class[_]](
  )

  private val testFlinkStreamingComponents = mutable.MutableList[Class[_]](
    // Input
    classOf[flink.streaming.connectors.ReflexSocketConnector],
    classOf[flink.streaming.connectors.ReflexKafkaConnector],
    classOf[flink.streaming.connectors.ReflexNullSourceConnector],

    // Output
    classOf[flink.streaming.connectors.ReflexNullConnector],
    classOf[flink.streaming.connectors.EventSocketSource],
    classOf[flink.streaming.connectors.EventSocketTwoSource],
    classOf[flink.streaming.connectors.EventSocketSink],

    // Flow control
    classOf[flink.streaming.general.TwoDup],
    classOf[flink.streaming.general.ReflexTwoUnionComponent],

    // Parsing
    classOf[flink.streaming.parsers.ReflexStringToBreezeVectorComponent],
    classOf[flink.streaming.parsers.ReflexStringToLabeledVectorComponent],
    classOf[flink.streaming.dummy.TestArgsComponent],
    classOf[flink.streaming.dummy.TestComponentWithDefaultInput],
    classOf[flink.streaming.dummy.TestComponentWithThreeDefaultInputs],
    classOf[flink.streaming.dummy.TestComponentWithDefaultOutput1],
    classOf[flink.streaming.dummy.TestComponentWithDefaultOutput2],
    classOf[flink.streaming.dummy.TestComponentWithTwoDefaultOutputs],
    classOf[flink.streaming.dummy.TestComponentWithDefaultOutput],
    classOf[flink.streaming.dummy.TestComponentWithDefaultOutputNull],
    classOf[flink.streaming.dummy.TestComponentWithDefaultInputAndSide1],
    classOf[flink.streaming.dummy.TestComponentWithDefaultInputAndSide2],
    classOf[flink.streaming.dummy.TestNullConnector],
    classOf[flink.streaming.dummy.TestComponentSource],
    classOf[flink.streaming.dummy.TestDataItemBSource],
    classOf[flink.streaming.dummy.TestNeedDataItemAInput])

  def apply(testMode: Boolean = false): FlinkStreamingComponentFactory = {
    if (!testMode) {
      new FlinkStreamingComponentFactory(flinkStreamingComponents)
    } else {
      val l = flinkStreamingComponents ++ testFlinkStreamingComponents
      new FlinkStreamingComponentFactory(l)
    }
  }
}
