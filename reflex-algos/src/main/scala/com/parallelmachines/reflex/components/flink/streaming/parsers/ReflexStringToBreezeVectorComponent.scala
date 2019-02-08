package com.parallelmachines.reflex.components.flink.streaming.parsers

import breeze.linalg.{DenseVector => BreezeDenseVector}
import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.mlpiper.parameters.common.ArgumentParameterTool
import org.mlpiper.parameters.ml.Attributes
import org.mlpiper.parameters.parsing._
import org.apache.flink.streaming.scala.examples.flink.utils.functions.conversion.StringToIndexedBreezeVectorFlatMap
import org.mlpiper.utils.ParameterIndices

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexStringToBreezeVectorComponent extends FlinkStreamingComponent {
  var attributes: Int = _
  var sep: String = _
  var indices: ParameterIndices = _

  override val isSource = false
  override val group = ComponentsGroups.dataParsers
  override val label = "String to Vector"
  override val description = "Convert a string to a vector."
  override val version = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[String],
    label = "String",
    description = "String of separated elements",
    group = ConnectionGroups.DATA)

  val output = ComponentConnection(
    tag = typeTag[BreezeDenseVector[Double]],
    label = "Vector",
    description = "Vector of attributes",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList(output)

  var params = new ArgumentParameterTool("String to Vector Component")
  params.add(Attributes)
  params.add(DataSeparator)
  params.add(IndicesRange)

  override lazy val paramInfo = s"""[${params.toJson}]"""

  override def configure(paramMap: Map[String, Any]): Unit = {
    params.initializeParameters(paramMap)

    sep = s"${Separator(params.get(DataSeparator))}"
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

    val flatMapFunction = new StringToIndexedBreezeVectorFlatMap(attributes, sep, indices)

    val dsOutput = dsInput.flatMap(flatMapFunction)
    ArrayBuffer(new DataWrapper(dsOutput))
  }
}
