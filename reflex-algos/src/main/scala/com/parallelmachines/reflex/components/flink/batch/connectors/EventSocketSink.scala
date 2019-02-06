package com.parallelmachines.reflex.components.flink.batch.connectors

import com.google.protobuf.ByteString
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.flink.batch.FlinkBatchComponent
import com.parallelmachines.reflex.components.flink.batch.functions.EventSocketSinkBatchOutputFormat
import com.parallelmachines.reflex.pipeline.{ComponentsGroups, _}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}

import scala.collection.mutable.ArrayBuffer

private class MapDataToEvent(eventTypeInfo: EventTypeInfo) extends MapFunction[String, ReflexEvent] {
  override def map(value: String): ReflexEvent = {
    ReflexEvent(eventTypeInfo.eventType,
      eventTypeInfo.eventLabel,
      ByteString.copyFrom(value.map(_.toByte).toArray))
  }
}

/**
  *
  * EventSocketSink performs multiplexing
  * of input DataSets into one output DataSet.
  * Incoming data will be wrapped into ReflexEvent with EventType
  *
  */
class EventSocketSink extends FlinkBatchComponent {
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

  override def materialize(env: ExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    val host = if (mlObjectSocketHost.value == "") null else mlObjectSocketHost.value
    val port = mlObjectSocketSinkPort.value

    val outputFormat = new EventSocketSinkBatchOutputFormat(host, port)

    /** Iterate over all inputs and perform action according to eventType registered for this connection.
      * If connection is marked as forwardEvent, expected data is ReflexEvent itself, so no need to wrap it into ReflexEvent.
      * (Look at ModelAcceptedEventProducer)
      *
      * For all other cases, String is expected so wrap it into ReflexEvent. */
    for (i <- dsArr.indices) {
      val eventTypeInfo = inputTypes(i).eventTypeInfo.get
      if (eventTypeInfo.forwardEvent) {
        dsArr(i).data[DataSet[ReflexEvent]].output(outputFormat)
      } else {
        dsArr(i).data[DataSet[String]].map(new MapDataToEvent(eventTypeInfo)).output(outputFormat)
      }
    }

    ArrayBuffer[DataWrapperBase]()
  }
}