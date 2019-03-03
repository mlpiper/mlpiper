package com.parallelmachines.reflex.test.components

import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent.EventType
import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.components.flink.streaming.StreamExecutionEnvironment
import com.parallelmachines.reflex.components.flink.streaming.connectors.{EventSocketSource, ReflexNullConnector}
import com.parallelmachines.reflex.pipeline.{ComponentsGroups, ConnectionGroups}
import com.parallelmachines.reflex.pipeline._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class TestComponentWithDefaultInputSingleton1 extends FlinkStreamingComponent {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "Test args component"
  override val description: String = "Test configuration parameters"
  override val version: String = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[Any],
    defaultComponentClass = Some(classOf[EventSocketSource]),
    eventTypeInfo = Some(EventTypeInfo(EventType.Model, Some("model_label"))),
    label = "Input",
    description = "Input",
    group = ConnectionGroups.OTHER)

  val output = ComponentConnection(
    tag = typeTag[Any],
    defaultComponentClass = Some(classOf[ReflexNullConnector]),
    label = "Output",
    description = "Output",
    group = ConnectionGroups.OTHER)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList(output)

  override def configure(paramMap: Map[String, Any]): Unit = {
  }

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {
    ArrayBuffer[DataWrapperBase](dsArr(0))
  }
}