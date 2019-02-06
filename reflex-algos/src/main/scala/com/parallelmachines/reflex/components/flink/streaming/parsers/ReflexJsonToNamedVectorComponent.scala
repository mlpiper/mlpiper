package com.parallelmachines.reflex.components.flink.streaming.parsers

import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.scala.examples.clustering.math.ReflexNamedVector
import org.apache.flink.streaming.scala.examples.functions.conversion.JsonToNamedVectorFlatMap

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexJsonToNamedVectorComponent extends FlinkStreamingComponent {

  override val isSource = false
  override val group = ComponentsGroups.dataParsers
  override val label = "Json to Named vector"
  override val description = "Convert JSON string to Named Vector (A Named Vector is similar to " +
    "regular vector, but with names for each attributes of the vector)"
  override val version = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[String],
    label = "Json",
    description = "Json object containing map",
    group = ConnectionGroups.DATA)

  val output = ComponentConnection(
    tag = typeTag[ReflexNamedVector],
    label = "Named Vector",
    description = "Vector of named elements",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList(output)

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    val dsInput = dsArr(0).data[DataStream[String]]

    val jsonToNamedVectorFunction = new JsonToNamedVectorFlatMap()

    val dsOutput = dsInput.flatMap(jsonToNamedVectorFunction)
    ArrayBuffer(new DataWrapper(dsOutput))
  }
}
