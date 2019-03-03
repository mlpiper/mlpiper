package com.parallelmachines.reflex.components.flink.streaming.connectors

import com.parallelmachines.reflex.components.flink.streaming.{FlinkStreamingComponent, StreamExecutionEnvironment}
import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.pipeline.ComponentsGroups
import com.parallelmachines.reflex.pipeline._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class EventSocketSource extends FlinkStreamingComponent {
  override val isSource = true
  override val isSingleton = true
  override lazy val isVisible = false

  override val group = ComponentsGroups.connectors
  override val label = "EventSocketSource"
  override val description = "Receives events from socket"
  override val version = "1.0.0"

  override val inputTypes: ConnectionList = ConnectionList.empty()
  override var outputTypes: ConnectionList = ConnectionList.empty()

  val mlObjectSocketHost = ComponentAttribute("mlObjectSocketHost", "", "Events source socket host", "Host to read ML health events from")
  val mlObjectSocketSourcePort = ComponentAttribute("mlObjectSocketSourcePort", -1, "Events source socket port", "Port to read ML health events from").setValidator(_ >= 0)

  attrPack.add(mlObjectSocketHost, mlObjectSocketSourcePort)

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    ArrayBuffer[DataWrapperBase]()
  }
}