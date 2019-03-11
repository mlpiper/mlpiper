package com.parallelmachines.reflex.components.spark.batch.connectors

import com.google.protobuf.ByteString
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent
import org.mlpiper.output.SocketSinkSingleton
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.mlpiper.infrastructure.{ComponentsGroups, ConnectionList, DataWrapperBase, EventTypeInfo}

import scala.collection.mutable.ArrayBuffer

object SparkMapDataToEvent {
  def map(data: String, eventTypeInfo: EventTypeInfo): ReflexEvent = {
    ReflexEvent(eventTypeInfo.eventType,
      eventTypeInfo.eventLabel,
      ByteString.copyFrom(data.map(_.toByte).toArray))
  }
}

/**
  *
  * EventSocketSink performs multiplexing
  * of multiple input RDD[String] into single RDD[ReflexEvent].
  * Incoming data will be wrapped into ReflexEvent with EventType
  *
  */
class EventSocketSink extends SparkBatchComponent {
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

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {
    val host = if (mlObjectSocketHost.value == "") null else mlObjectSocketHost.value
    val port = mlObjectSocketSinkPort.value

    SocketSinkSingleton.startClient(host, port, writeMode = true)
    var rddEvents: RDD[ReflexEvent] = null

    for (i <- dsArr.indices) {
      val eventTypeInfo = inputTypes(i).eventTypeInfo.get
      if (eventTypeInfo.forwardEvent) {
        rddEvents = dsArr(i).data[RDD[ReflexEvent]]
      } else {
        rddEvents = dsArr(i).data[RDD[String]].map(x => SparkMapDataToEvent.map(x, eventTypeInfo))
      }

      rddEvents.collect().foreach(SocketSinkSingleton.putRecord(_))
    }

    SocketSinkSingleton.stopClient()
    ArrayBuffer[DataWrapperBase]()
  }
}
