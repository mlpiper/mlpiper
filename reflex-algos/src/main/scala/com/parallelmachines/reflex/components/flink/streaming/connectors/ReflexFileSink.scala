package com.parallelmachines.reflex.components.flink.streaming.connectors

import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexFileSink extends FlinkStreamingComponent {

  override val isSource = false
  override val group: String = ComponentsGroups.sinks
  override val label = "File Output"
  override val description = "Sends Data To Provided File Name"
  override val version = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[Any],
    label = "Data",
    description = "Data to sink",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList.empty()

  val fileName = ComponentAttribute("fileName", "", "File Name To Sink Data", "Component Will Sink Input Data To Provided File").setValidator(!_.isEmpty)

  attrPack.add(fileName)

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    dsArr(0)
      .data[DataStream[Any]]()
      .writeAsText(fileName.value, FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)

    ArrayBuffer[DataWrapperBase]()
  }
}



