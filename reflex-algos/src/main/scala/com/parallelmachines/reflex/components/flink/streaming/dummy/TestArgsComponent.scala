package com.parallelmachines.reflex.components.flink.streaming.dummy

import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.flink.streaming.{FlinkStreamingComponent, StreamExecutionEnvironment}
import com.parallelmachines.reflex.pipeline._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._


class TestArgsComponent extends FlinkStreamingComponent {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "Test args component"
  override val description: String = "Test configuration parameters"
  override val version: String = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[Any],
    label = "Input",
    description = "Input",
    group = ConnectionGroups.OTHER)

  val output = ComponentConnection(
    tag = typeTag[Any],
    label = "Output",
    description = "Output",
    group = ConnectionGroups.OTHER)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList(output)

  val boolValArg = ComponentAttribute("bool-val", true, "Bool Arg", "Dummy boolean value")
  val intValArg = ComponentAttribute("int-val", 0, "Int Arg", "Dummy integer value")
  val longValArg = ComponentAttribute("long-val", 0.toLong, "Long Arg", "Dummy long value")
  val doubleValArg = ComponentAttribute("long-val", 0.1, "Double Arg", "Dummy double value")
  val stringValArg = ComponentAttribute("string-val", "", "String Arg", "Dummy string value")
  attrPack.add(boolValArg, intValArg, longValArg, doubleValArg, stringValArg)

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {
    return ArrayBuffer[DataWrapperBase]()
  }
}
