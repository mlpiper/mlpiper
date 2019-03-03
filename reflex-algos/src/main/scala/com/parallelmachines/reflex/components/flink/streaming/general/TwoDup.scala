package com.parallelmachines.reflex.components.flink.streaming.general

import com.parallelmachines.reflex.components.flink.streaming.{FlinkStreamingComponent, StreamExecutionEnvironment}
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._


class TwoDup extends FlinkStreamingComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.flowShaping
  override val label = "Duplicate data stream"
  override val description = "Duplicates data stream"
  override val version = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[Any],
    label = "Data",
    description = "Data to duplicate",
    group = ConnectionGroups.DATA)

  val output1 = ComponentConnection(
    tag = typeTag[Any],
    label = "Data1",
    description = "Duplicated data1",
    group = ConnectionGroups.DATA)

  val output2 = ComponentConnection(
    tag = typeTag[Any],
    label = "Data2",
    description = "Duplicated data2",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList(output1, output2)

  @throws(classOf[Exception])
  override def validateAndPropagateIncomingTypes(incomingTypes: ConnectionList): Unit = {
    validateNumberOfIncoming(incomingTypes)
    // Dup component can accept anything - but once got input infer on the output
    outputTypes = ConnectionList(incomingTypes(0), incomingTypes(0))
  }

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    ArrayBuffer[DataWrapperBase]()
  }
}