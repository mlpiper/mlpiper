package com.parallelmachines.reflex.components.flink.streaming.dummy

import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class TestComponentSource extends FlinkStreamingComponent {
  override val isSource: Boolean = true
  override val group: String = ComponentsGroups.connectors
  override val label: String = "Test Source"
  override val description: String = "Testing purposes source component"
  override val version: String = "1.0.0"

  val output = ComponentConnection(
    tag = typeTag[Any],
    label = "Output",
    description = "Output",
    group = ConnectionGroups.OTHER)

  override val inputTypes: ConnectionList = ConnectionList.empty()
  override var outputTypes: ConnectionList = ConnectionList(output)

  override lazy val paramInfo: String = s"""[]""".stripMargin

  override def configure(paramMap: Map[String, Any]): Unit = {

  }

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {
    return ArrayBuffer[DataWrapperBase]()
  }
}
