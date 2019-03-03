package com.parallelmachines.reflex.components.flink.streaming.connectors

import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.pipeline.ComponentsGroups
import com.parallelmachines.reflex.pipeline._
import com.parallelmachines.reflex.components.flink.streaming.StreamExecutionEnvironment

import scala.collection.mutable.ArrayBuffer

class EventSocketSink extends FlinkStreamingComponent {
  override val isSource = false
  override val isSingleton = true
  override lazy val isVisible = false

  override val group = ComponentsGroups.connectors
  override val label = "EventSocketSink"
  override val description = "Sends events to socket"
  override val version = "1.0.0"

  override val inputTypes: ConnectionList = ConnectionList.empty()
  override var outputTypes: ConnectionList = ConnectionList.empty()

  val mlObjectSocketHost = ComponentAttribute("mlObjectSocketHost", "", "Events sink socket host", "Host to send ML health events to")
  val mlObjectSocketSinkPort = ComponentAttribute("mlObjectSocketSinkPort", -1, "Events sink socket port", "Port to send ML health events to").setValidator(_ >= 0)

  attrPack.add(mlObjectSocketHost, mlObjectSocketSinkPort)

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    ArrayBuffer[DataWrapperBase]()
  }
}