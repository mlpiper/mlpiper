package com.parallelmachines.reflex.components.flink.streaming.connectors

import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.flink.streaming.{FlinkStreamingComponent, StreamExecutionEnvironment}
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}

import scala.reflect.runtime.universe._
import scala.collection.mutable.ArrayBuffer

/**
  * ReflexSocketConnector reads strings from socket.
  */
class ReflexSocketConnector extends FlinkStreamingComponent {

  override val isSource = true
  override val group: String = ComponentsGroups.connectors
  override val label = "Socket source"
  override val description = "Reads strings from socket"
  override val version = "1.0.0"

  val output = ComponentConnection(
    tag = typeTag[String],
    label = "Data",
    description = "Data received from socket",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList.empty()
  override var outputTypes: ConnectionList = ConnectionList(output)

  val hostArg = ComponentAttribute("socketSourceHost", "", "Source socket host", "Host to connect to")
  val portArg = ComponentAttribute("socketSourcePort", -1, "Source socket port", "Port number to connect to").setValidator(_ >= 0)

  attrPack.add(hostArg, portArg)

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    ArrayBuffer[DataWrapperBase]()
  }
}
