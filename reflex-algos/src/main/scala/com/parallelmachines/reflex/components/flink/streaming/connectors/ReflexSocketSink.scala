package com.parallelmachines.reflex.components.flink.streaming.connectors

import com.parallelmachines.reflex.components.ComponentAttribute
import org.apache.flink.streaming.scala.examples.flink.utils.functions.sink.SocketSinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

/**
  * ReflexSocketSink receives data of Any type and sends it as a String.
  * */
class ReflexSocketSink extends FlinkStreamingComponent {

  val isSource = false

  override val group: String = ComponentsGroups.sinks
  override val label = "Socket sink"
  override val description = "Output to socket as a string"
  override val version = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[Any],
    label = "Data",
    description = "Data to send to socket",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList.empty()

  val hostArg = ComponentAttribute("socketSinkHost", "", "Sink socket host", "Host to connect to")
  val portArg = ComponentAttribute("socketSinkPort", -1, "Sink socket port", "Port number to connect to").setValidator(_ >= 0)

  attrPack.add(hostArg, portArg)

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {

    val dsInput = dsArr(0).data[DataStream[Any]]
    val hostValue = hostArg.value
    val portValue = portArg.value

    val socketSinkFunction = new SocketSinkFunction[Any](hostValue, portValue)
    dsInput.addSink(socketSinkFunction)
    ArrayBuffer[DataWrapperBase]()
  }
}
