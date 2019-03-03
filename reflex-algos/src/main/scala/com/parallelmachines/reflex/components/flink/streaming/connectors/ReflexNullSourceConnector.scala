package com.parallelmachines.reflex.components.flink.streaming.connectors

import com.parallelmachines.reflex.components.flink.streaming.{FlinkStreamingComponent, StreamExecutionEnvironment}
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, ConnectionList, _}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexNullSourceConnector extends FlinkStreamingComponent {
  val isSource = true

  override val group: String = ComponentsGroups.connectors
  override val label = "Null"
  override val description = "Stub for an input stream"
  override val version = "1.0.0"
  override lazy val paramInfo: String =
    """[]""".stripMargin

  override val inputTypes: ConnectionList = ConnectionList.empty()

  val output = ComponentConnection(
    tag = typeTag[Any],
    label = "Data",
    description = "Empty data",
    group = ConnectionGroups.DATA)

  override var outputTypes: ConnectionList = ConnectionList(output)

  override def configure(paramMap: Map[String, Any]): Unit = {
  }

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    ArrayBuffer[DataWrapperBase]()
  }
}



