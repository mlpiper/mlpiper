package com.parallelmachines.reflex.components.flink.streaming.dummy

import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class TestComponentWithDefaultOutputNull extends FlinkStreamingComponent {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "Test Default Input"
  override val description: String = "Testing component with default input"
  override val version: String = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[Any],
    label = "Input",
    description = "Input",
    group = ConnectionGroups.OTHER)

  val output = ComponentConnection(
    tag = typeTag[Any],
    defaultComponentClass = Some(classOf[TestNullConnector]),
    label = "Input",
    description = "Input",
    group = ConnectionGroups.OTHER)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList(output)

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {
    return ArrayBuffer[DataWrapperBase]()
  }
}
