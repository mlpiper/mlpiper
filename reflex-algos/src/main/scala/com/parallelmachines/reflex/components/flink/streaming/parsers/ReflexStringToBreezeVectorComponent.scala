package com.parallelmachines.reflex.components.flink.streaming.parsers

import breeze.linalg.{DenseVector => BreezeDenseVector}
import com.parallelmachines.reflex.components.flink.streaming.{FlinkStreamingComponent, StreamExecutionEnvironment}
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexStringToBreezeVectorComponent extends FlinkStreamingComponent {
  var attributes: Int = _
  var sep: String = _

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

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    ArrayBuffer[DataWrapperBase]()
  }
}
