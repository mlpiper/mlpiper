package com.parallelmachines.reflex.components.flink.streaming.connectors

import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.components.flink.streaming.functions.HealthEventStreamSocketSource
import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.pipeline.ComponentsGroups
import com.parallelmachines.reflex.pipeline._
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

private class EventTypeFilterFunction(eventTypeInfo: EventTypeInfo) extends FilterFunction[ReflexEvent] {
  override def filter(value: ReflexEvent): Boolean = {
    (value.eventType == eventTypeInfo.eventType) &&
      (value.eventLabel == eventTypeInfo.eventLabel)
  }
}

private class ExtractEventData() extends MapFunction[ReflexEvent, String] {
  override def map(value: ReflexEvent): String = {
    new String(value.data.toByteArray)
  }
}

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
    var retArray = mutable.ArrayBuffer[DataWrapperBase]()
    val host = if (mlObjectSocketHost.value == "") null else mlObjectSocketHost.value
    val port = mlObjectSocketSourcePort.value
    val objSourceStream = env.addSource(new HealthEventStreamSocketSource(host, port)).setParallelism(1)

    for (outputType <- outputTypes) {
      val eventTypeInfo = outputType.eventTypeInfo.get
      val filteredSourceStream = objSourceStream.filter(new EventTypeFilterFunction(eventTypeInfo))
      /*
         When connection is marked as forwardEvent, ReflexEvent object is expected to be received by component which is next in pipeline.
         So data will not be extracted from the object, and it will be just forwarded.
      */
      if (eventTypeInfo.forwardEvent) {
        retArray += new DataWrapper(filteredSourceStream)
      } else {
        retArray += new DataWrapper(filteredSourceStream.map(new ExtractEventData()))
      }
    }
    retArray
  }
}