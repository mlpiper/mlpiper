package com.parallelmachines.reflex.components.flink.streaming.dummy

import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class TestNullConnector extends FlinkStreamingComponent {
  val isSource = false

  override val group: String = ComponentsGroups.sinks
  override val label = "Null"
  override val description = "Sends data to nothing"
  override val version = "1.0.0"
  override lazy val paramInfo: String =
    """[]""".stripMargin

  val input = ComponentConnection(
    tag = typeTag[Any],
    label = "Data",
    description = "Data to ignore",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList.empty()

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    ArrayBuffer[DataWrapperBase]()
  }
}



