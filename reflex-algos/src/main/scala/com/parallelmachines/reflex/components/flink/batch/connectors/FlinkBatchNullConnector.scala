package com.parallelmachines.reflex.components.flink.batch.connectors

import com.parallelmachines.reflex.components.flink.batch.FlinkBatchComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.scala.examples.common.serialize.NullSinkForFlink

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class FlinkBatchNullConnector extends FlinkBatchComponent {
  val isSource = false

  override val group: String = ComponentsGroups.sinks
  override val label = "Null"
  override val description = "Sends Data To Nothing (Literally)"
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

  override def configure(paramMap: Map[String, Any]): Unit = {
  }

  override def materialize(env: ExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String) = {
    dsArr(0)
      .data[DataSet[Any]]
      .output(new NullSinkForFlink[Any])

    ArrayBuffer[DataWrapperBase]()
  }
}



