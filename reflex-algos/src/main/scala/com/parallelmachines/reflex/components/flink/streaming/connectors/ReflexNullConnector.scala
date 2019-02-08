package com.parallelmachines.reflex.components.flink.streaming.connectors

import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.scala.examples.flink.utils.NullSinkForFlink

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexNullConnector extends FlinkStreamingComponent {

  override val isSource = false
  override val group: String = ComponentsGroups.sinks
  override val label = "Null Sink"
  override val description = "Sends data to nothing"
  override val version = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[Any],
    label = "Data",
    description = "Data to ignore",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList.empty()

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    dsArr(0)
      .data[DataStream[Any]]()
      .writeUsingOutputFormat(new NullSinkForFlink[Any])

    ArrayBuffer[DataWrapperBase]()
  }
}



