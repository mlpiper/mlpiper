package com.parallelmachines.reflex.components.flink.streaming.dummy

import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class TestComponentWithThreeDefaultInputs extends FlinkStreamingComponent {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "Test Default Input"
  override val description: String = "Testing component with default input"
  override val version: String = "1.0.0"

  val input1 = ComponentConnection(
    tag = typeTag[Any],
    defaultComponentClass = Some(classOf[TestComponentSource]),
    label = "Input1",
    description = "Input1",
    group = ConnectionGroups.OTHER)

  val input2 = ComponentConnection(
    tag = typeTag[Any],
    defaultComponentClass = Some(classOf[TestComponentSource]),
    label = "Input2",
    description = "Input2",
    group = ConnectionGroups.OTHER)

  val input3 = ComponentConnection(
    tag = typeTag[Any],
    defaultComponentClass = Some(classOf[TestComponentSource]),
    label = "Input3",
    description = "Input3",
    group = ConnectionGroups.OTHER)

  override val inputTypes: ConnectionList = ConnectionList(input1, input2, input3)
  override var outputTypes: ConnectionList = ConnectionList.empty()

  override lazy val paramInfo: String = s"""[]""".stripMargin

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {
    return ArrayBuffer[DataWrapperBase]()
  }
}
