package com.parallelmachines.reflex.components.flink.batch.connectors

import com.parallelmachines.reflex.components.flink.batch.FlinkBatchComponent
import com.parallelmachines.reflex.pipeline.{ComponentsGroups, ConnectionGroups}
import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.pipeline._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.scala.examples.common.serialize.BatchModelOutputFormat

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class FlinkBatchMLHealthModelFileSink extends FlinkBatchComponent {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.sinks
  override val label: String = "ML Health Model - File Output"
  override val description: String = "Component is responsible for saving calculated ML Health model to a provided file"
  override val version: String = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[Any],
    label = "Health Model",
    description = "Health Model to save to file",
    group = ConnectionGroups.MODEL)

  val inputTypes: ConnectionList = ConnectionList(input)
  var outputTypes: ConnectionList = ConnectionList.empty()

  val modelFilePath = ComponentAttribute("healthStatFilePath", "", "ML Health model file path", "Outputs ML Health model to file path")
  attrPack.add(modelFilePath)

  override def materialize(env: ExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {
    dsArr(0)
      .data[DataSet[String]]
      .output(new BatchModelOutputFormat[String](modelFilePath.value))
      .setParallelism(1)

    ArrayBuffer[DataWrapperBase]()
  }
}