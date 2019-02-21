package com.parallelmachines.reflex.components.flink.streaming.parsers

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import org.apache.flink.streaming.scala.examples.functions.conversion.StringToIndexedLabeledVectorFlatMap
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.streaming.scala.examples.clustering.math.LabeledVector
import org.apache.flink.streaming.scala.examples.common.parameters.ml.Attributes
import org.apache.flink.streaming.scala.examples.common.parameters.parsing._
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterTool
import org.apache.flink.streaming.scala.examples.common.parsing.ParameterIndices

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexStringToLabeledVectorComponent extends FlinkStreamingComponent {
  var attributes: Int = _
  var sep: String = _
  var labelIndex: Option[Int] = None
  var timestampIndex: Option[Int] = None
  var indices: ParameterIndices = _

  override val isSource = false
  override val group = ComponentsGroups.dataParsers
  override val label = "String to Labeled Vector"
  override val description = "Convert a string to a labeled vector."
  override val version = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[String],
    label = "String",
    description = "String of separated elements",
    group = ConnectionGroups.DATA)

  val output = ComponentConnection(
    tag = typeTag[LabeledVector[Double]],
    label = "Labeled Vector",
    description = "Labeled vector of attributes",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList(output)

  var params = new ArgumentParameterTool("String to LabeledVector Component")
  params.add(Attributes)
  params.add(DataSeparator)
  params.add(LabelIndex)
  params.add(TimestampIndex)
  params.add(IndicesRange)

  override lazy val paramInfo = s"""[${params.toJson()}]"""

  override def configure(paramMap: Map[String, Any]): Unit = {
    params.initializeParameters(paramMap)

    sep = s"${Separator(params.get(DataSeparator))}"
    labelIndex = params.getOption(LabelIndex)
    timestampIndex = params.getOption(TimestampIndex)
    attributes = params.get(Attributes)

    val indicesRange = params.getOption(IndicesRange)
    if (indicesRange.isDefined) {
      indices = new ParameterIndices(indicesRange.get)
    } else {
      indices = new ParameterIndices(attributes)
    }
  }

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    val dsInput = dsArr(0).data[DataStream[String]]

    val flatMapFunction = new StringToIndexedLabeledVectorFlatMap(
      attributes, sep, indices, labelIndex, timestampIndex)

    val dsOutput = dsInput.flatMap(flatMapFunction)
    ArrayBuffer(new DataWrapper(dsOutput))
  }
}