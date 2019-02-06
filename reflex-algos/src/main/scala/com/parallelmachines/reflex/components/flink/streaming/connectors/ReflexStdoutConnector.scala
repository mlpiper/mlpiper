package com.parallelmachines.reflex.components.flink.streaming.connectors

import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexStdoutConnector extends FlinkStreamingComponent {
  val isSource = false

  override val group: String = ComponentsGroups.sinks
  override val label = "Console"
  override val description = "ReflexStdoutConnector - desc"
  override val version = "1.0.0"
  override lazy val paramInfo: String =
    """[]""".stripMargin

  val input = ComponentConnection(
    tag = typeTag[Any],
    label = "Data",
    description = "Data to print",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList.empty()

  override def configure(paramMap: Map[String, Any]): Unit = {
  }

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    dsArr(0).data[DataStream[Any]].print()
    ArrayBuffer[DataWrapperBase]()
  }
}



