package com.parallelmachines.reflex.components.flink.streaming.parsers

import com.parallelmachines.reflex.components.flink.streaming.{FlinkStreamingComponent, StreamExecutionEnvironment}
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.mlpiper.datastructures.LabeledVector
import org.mlpiper.utils.ParameterIndices

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexStringToLabeledVectorComponent extends FlinkStreamingComponent {
  var attributes: Int = _
  var sep: String = _
  var labelIndex: Option[Int] = None
  var timestampIndex: Option[Int] = None

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

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    ArrayBuffer[DataWrapperBase]()
  }
}
