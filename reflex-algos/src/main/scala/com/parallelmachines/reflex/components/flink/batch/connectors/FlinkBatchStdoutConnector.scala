package com.parallelmachines.reflex.components.flink.batch.connectors

import com.parallelmachines.reflex.components.flink.batch.FlinkBatchComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import scala.reflect.runtime.universe._
import scala.collection.mutable.ArrayBuffer


class FlinkBatchStdoutConnector extends FlinkBatchComponent {
  val isSource = false

  val group: String = ComponentsGroups.sinks
  val label = "Console"
  val description = "Print output to stdout"
  val version = "1.0.0"
  override lazy val paramInfo = """[]""".stripMargin

  val input = ComponentConnection(
    tag = typeTag[Any],
    label = "Data",
    description = "Data to print",
    group = ConnectionGroups.DATA)

  val inputTypes: ConnectionList = ConnectionList(input)
  var outputTypes: ConnectionList = ConnectionList.empty()

  override def configure(paramMap: Map[String, Any]): Unit = {
  }

  override def materialize(env: ExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    dsArr(0).data[DataSet[Any]].print()
    ArrayBuffer[DataWrapperBase]()
  }
}
