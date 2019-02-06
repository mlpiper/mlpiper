package com.parallelmachines.reflex.components.flink.streaming.connectors

import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import com.parallelmachines.reflex.components.flink.streaming.functions.HealthEventStreamSocketSource
import com.parallelmachines.reflex.pipeline.{CanaryConfig, DataWrapper, DataWrapperBase, EventTypeInfo}
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

private class EventTypeFilterFunctionWithCustomLabel(eventTypeInfo: EventTypeInfo, label: String) extends FilterFunction[ReflexEvent] {
  override def filter(value: ReflexEvent): Boolean = {
    (value.eventType == eventTypeInfo.eventType) &&
      (value.eventLabel == Some(label))
  }
}

/**
  * EventSocketTwoSource is a subclass of EventSocketSource.
  * Which produces exactly 2 outputs.
  *
  * Configured to work with labels provided in systemConfig.canaryConfig
  */
class EventSocketTwoSource extends EventSocketSource {

  override val label = "Event Socket Source for Comparator"
  override val description = "Receives events and separates them into two streams"
  override val version = "1.0.0"
  override lazy val isVisible = false

  var label1 = ""
  var label2 = ""

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)
    require(paramMap.contains("canaryConfig"), "canaryConfig section was not found in systemConfig")
    val cc = paramMap("canaryConfig").asInstanceOf[CanaryConfig]
    require(cc.canaryLabel1.isDefined && cc.canaryLabel2.isDefined, "canaryConfig section must contain two labels")
    label1 = cc.canaryLabel1.get
    label2 = cc.canaryLabel2.get
  }

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    require(outputTypes.size == 2, "EventSocketComparatorSource must contain only two outputs")
    var retArray = mutable.ArrayBuffer[DataWrapperBase]()
    val host = if (mlObjectSocketHost.value == "") null else mlObjectSocketHost.value
    val port = mlObjectSocketSourcePort.value
    val objSourceStream = env.addSource(new HealthEventStreamSocketSource(host, port)).setParallelism(1)

    retArray += new DataWrapper(objSourceStream.filter(new EventTypeFilterFunctionWithCustomLabel(outputTypes(0).eventTypeInfo.get, label1)).map(new ExtractEventData()))
    retArray += new DataWrapper(objSourceStream.filter(new EventTypeFilterFunctionWithCustomLabel(outputTypes(1).eventTypeInfo.get, label2)).map(new ExtractEventData()))

    retArray
  }
}