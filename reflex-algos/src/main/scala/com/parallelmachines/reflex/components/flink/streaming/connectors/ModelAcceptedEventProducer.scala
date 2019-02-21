package com.parallelmachines.reflex.components.flink.streaming.connectors

import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent.EventType
import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

private class MapModelEventToModelReceivedEvent() extends MapFunction[ReflexEvent, ReflexEvent] {
  override def map(value: ReflexEvent): ReflexEvent = {
    ReflexEvent(EventType.ModelAccepted, value.eventLabel, value.data, value.modelId)
  }
}

/**
  * Component receives Model event and sends out ModelAccepted event.
  */
@deprecated
class ModelAcceptedEventProducer extends FlinkStreamingComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.connectors
  override val label = "Model Received Confirmator"
  override val description = "Send ModelAccepted event when model is received by pipeline"
  override val version = "1.0.0"
  override lazy val isVisible = false

  val input = ComponentConnection(
    tag = typeTag[ReflexEvent],
    defaultComponentClass = Some(classOf[EventSocketSource]),
    eventTypeInfo = Some(EventDescs.ModelEventRepeat),
    label = "Model generated event",
    description = "Model generated event",
    group = ConnectionGroups.OTHER)

  val output = ComponentConnection(
    tag = typeTag[ReflexEvent],
    defaultComponentClass = Some(classOf[EventSocketSink]),
    eventTypeInfo = Some(EventDescs.ModelReceived),
    label = "Model accepted event",
    description = "Model accepted event",
    group = ConnectionGroups.OTHER)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList(output)

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    val dsOutput = dsArr(0).data[DataStream[ReflexEvent]].map(new MapModelEventToModelReceivedEvent())
    ArrayBuffer(new DataWrapper(dsOutput))
  }
}