package com.parallelmachines.reflex.test.components

import breeze.linalg.{DenseVector => BreezeDenseVector}
import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.streaming.api.datastream.{DataStream, _}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.reflect.runtime.universe._
import scala.collection.mutable.ArrayBuffer

class TestVectorToString extends FlinkStreamingComponent {
  override val isSource = false
  override val group = ComponentsGroups.dataParsers
  override val label = "TestVectorToString"
  override val description = "TestVectorToString"
  override val version = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[BreezeDenseVector[Double]],
    label = "Vector",
    description = "Vector of attributes",
    group = ConnectionGroups.DATA)

  val output = ComponentConnection(
    tag = typeTag[String],
    label = "String",
    description = "String of separated elements",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList(output)

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    val dsOutput = dsArr(0).data[DataStream[BreezeDenseVector[Double]]]
    ArrayBuffer(new DataWrapper(dsOutput))
  }
}