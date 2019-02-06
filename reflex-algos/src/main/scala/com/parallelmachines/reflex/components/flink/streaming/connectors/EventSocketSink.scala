package com.parallelmachines.reflex.components.flink.streaming.connectors

import com.google.protobuf.ByteString
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.components.flink.streaming.functions.HealthEventStreamSocketSink
import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.pipeline.ComponentsGroups
import com.parallelmachines.reflex.pipeline._
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import scala.collection.mutable.ArrayBuffer

private class MapDataToEvent(eventTypeInfo: EventTypeInfo) extends MapFunction[String, ReflexEvent] {
  override def map(value: String): ReflexEvent = {
    ReflexEvent(eventTypeInfo.eventType,
      eventTypeInfo.eventLabel,
      ByteString.copyFrom(value.map(_.toByte).toArray))
  }
}

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
    val host = if (mlObjectSocketHost.value == "") null else mlObjectSocketHost.value
    val port = mlObjectSocketSinkPort.value

    val socketSinkFunction = new HealthEventStreamSocketSink(host, port)
    val eventTypeInfo = inputTypes(0).eventTypeInfo.get
    var resStream: DataStream[ReflexEvent] = null

    /*
      When connection is marked as forwardEvent, ReflexEvent object is expected in the input stream.
      So it will not be wrapped into another event, but just forwarded.
    */
    if (eventTypeInfo.forwardEvent) {
      resStream = dsArr(0).data[DataStream[ReflexEvent]]
    } else {
      resStream = dsArr(0).data[DataStream[String]].map(new MapDataToEvent(eventTypeInfo))
    }

    for (i <- dsArr.indices.slice(1, dsArr.length)) {
      // eventType2 variable is created to avoid Serializable issue
      val eventTypeInfo2 = inputTypes(i).eventTypeInfo.get
      if (eventTypeInfo2.forwardEvent) {
        val dsArrItem = dsArr(i).data[DataStream[ReflexEvent]]
        resStream = resStream.union(dsArrItem)
      } else {
        val dsArrItem = dsArr(i).data[DataStream[String]]
        resStream = resStream.union(dsArrItem.map(new MapDataToEvent(eventTypeInfo2)))
      }
    }

    resStream.addSink(socketSinkFunction)
    ArrayBuffer[DataWrapperBase]()
  }
}