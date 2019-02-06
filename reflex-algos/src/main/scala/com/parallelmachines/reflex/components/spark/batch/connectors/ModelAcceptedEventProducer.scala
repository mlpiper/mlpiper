package com.parallelmachines.reflex.components.spark.batch.connectors

import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent.EventType
import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

/**
  * Component receives Model event and sends out ModelAccepted event.
  *
  * When this component is connected to EventSocketSource,
  * EventSocketSource will forward Model Event to here.
  *
  * Event type will be replaced to ModelAccepted, forwarded to Sink and sent back to ECO.
  */
class ModelAcceptedEventProducer extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.connectors
  override val label = "Model Received Confirmator"
  override val description = "Send ModelAccepted event when model is received by pipeline"
  override val version = "1.0.0"
  override lazy val isVisible = false

  val input = ComponentConnection(
    tag = typeTag[RDD[ReflexEvent]],
    defaultComponentClass = Some(classOf[EventSocketSource]),
    eventTypeInfo = Some(EventDescs.ModelEventRepeat),
    label = "Model generated event",
    description = "Model generated event",
    group = ConnectionGroups.OTHER)

  val output = ComponentConnection(
    tag = typeTag[RDD[ReflexEvent]],
    defaultComponentClass = Some(classOf[EventSocketSink]),
    eventTypeInfo = Some(EventDescs.ModelReceived),
    label = "Model accepted event",
    description = "Model accepted event",
    group = ConnectionGroups.OTHER)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList(output)

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    val dsOutput = dsArr(0).data[RDD[ReflexEvent]].map{event => ReflexEvent(EventType.ModelAccepted, event.eventLabel, event.data, event.modelId)}
    ArrayBuffer(new DataWrapper(dsOutput))
  }
}