package com.parallelmachines.reflex.components.flink.streaming.parsers

import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.scala.examples.clustering.math.ReflexPrediction

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexPredictionToJsonComponent extends FlinkStreamingComponent {
  override val isSource = false
  override val group = ComponentsGroups.dataParsers
  override val label = "Prediction to Json"
  override val description = "Convert a Prediction to a Json."
  override val version = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[ReflexPrediction],
    label = "Prediction",
    description = "Predictions",
    group = ConnectionGroups.PREDICTION)

  val output = ComponentConnection(
    tag = typeTag[String],
    label = "String",
    description = "Prediction encoded in json string",
    group = ConnectionGroups.PREDICTION)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList(output)

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    val dsInput = dsArr(0).data[DataStream[ReflexPrediction]]
    val dsOutput = dsInput.map(_.toJson)

    ArrayBuffer(new DataWrapper(dsOutput))
  }
}