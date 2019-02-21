package com.parallelmachines.reflex.components.flink.batch.parsers

import breeze.linalg.{DenseVector => BreezeDenseVector}
import com.parallelmachines.reflex.components.flink.batch.FlinkBatchComponent
import com.parallelmachines.reflex.components.flink.streaming.parsers.ReflexStringToBreezeVectorComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.scala.examples.common.parameters.ml.Attributes
import org.apache.flink.streaming.scala.examples.common.parameters.parsing.{DataSeparator, IndicesRange, Separator}
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterTool
import org.apache.flink.streaming.scala.examples.common.parsing.ParameterIndices
import org.apache.flink.streaming.scala.examples.functions.conversion.StringToIndexedBreezeVectorFlatMap
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe._
import scala.collection.mutable.ArrayBuffer

class ReflexStringToBreezeVectorBatch extends FlinkBatchComponent {
  var attributes: Int = _
  var sep: String = _
  var indices: ParameterIndices = _

  override val isSource = false
  override val group = ComponentsGroups.dataParsers
  override val label = "String to Vector"
  override val description = "Convert a string to a vector"
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

  override lazy val paramInfo = s"""[${params.toJson()}]"""

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

  override def materialize(env: ExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    val dsInput = dsArr(0).data[DataSet[String]]

    val flatMapFunction = new StringToIndexedBreezeVectorFlatMap(attributes, sep, indices)

    val dsOutput = dsInput.flatMap(flatMapFunction)
    ArrayBuffer(new DataWrapper(dsOutput))
  }
}