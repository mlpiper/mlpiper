package com.parallelmachines.reflex.components.flink.streaming.parsers

import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.scala.examples.clustering.math.ReflexNamedVector
import org.apache.flink.streaming.scala.examples.functions.conversion.StringToNamedVectorFlatMap

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexStringToNamedVectorComponent extends FlinkStreamingComponent {
  override val isSource = false
  override val group = ComponentsGroups.dataParsers
  override val label = "String to NamedVector"
  override val description = "Convert a string to a named vector. Do not supply column names."
  override val version = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[String],
    label = "String",
    description = "String of separated elements",
    group = ConnectionGroups.DATA)

  val output = ComponentConnection(
    tag = typeTag[ReflexNamedVector],
    label = "Labeled Vector",
    description = "Labeled Vector of attributes",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList(output)

  val labelIndex = ComponentAttribute("labelIndex", -1, "Label Index", "Index of the label in a vector, starts with 0", optional = true)
  val separator = ComponentAttribute("separator", ",", "Separator", "Column separator", optional = true)

  var dataSeparator = ComponentAttribute("separator", "comma", "Separator", "Element separator. Default: (comma)")
  dataSeparator.setOptions(List[(String, String)](("Comma (,)", "comma"), ("Semicolon (;)", "semicolon"), ("Space ( )", "space"), ("Backtick (`)", "backtick")))

  attrPack.add(labelIndex, separator)

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    val dsInput = dsArr(0).data[DataStream[String]]

    val flatMapFunction = new StringToNamedVectorFlatMap(labelIndex.value, separator.value)

    val dsOutput = dsInput.flatMap(flatMapFunction)
    ArrayBuffer(new DataWrapper(dsOutput))
  }
}