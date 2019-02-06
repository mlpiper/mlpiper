package com.parallelmachines.reflex.components.flink.streaming.connectors

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.pipeline._
import org.apache.flink.contrib.streaming.scala.utils.DataStreamUtils

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexCollectConnector extends FlinkStreamingComponent {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.sinks
  override val label: String = "Stream Collector"
  override val description: String = "Collects a stream and exposes to CollectedData object API"
  override val version: String = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[Any],
    label = "Data",
    description = "Data to collect",
    group = ConnectionGroups.DATA)

  val inputTypes: ConnectionList = ConnectionList(input)
  var outputTypes: ConnectionList = ConnectionList.empty()

  val resultKey = ComponentAttribute("resultKey", "", "Result Key", "Key to put result")
  attrPack.add(resultKey)

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {
    CollectedData.set(resultKey.value, DataStreamUtils(dsArr(0).data[DataStream[Any]]).collect())
    ArrayBuffer[DataWrapperBase]()
  }
}