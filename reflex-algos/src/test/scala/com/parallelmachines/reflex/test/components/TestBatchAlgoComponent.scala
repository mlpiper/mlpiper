package com.parallelmachines.reflex.test.components

import com.parallelmachines.reflex.components.flink.batch.FlinkBatchComponent
import com.parallelmachines.reflex.components.flink.streaming.algorithms.{ModelBehavior, ModelBehaviorType}
import com.parallelmachines.reflex.pipeline.{ComponentConnection, ComponentsGroups, ConnectionGroups, _}
import org.apache.flink.api.scala.ExecutionEnvironment
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class TestBatchAlgoComponent extends FlinkBatchComponent with ModelBehavior {
  override val isSource = false
  override val group: String = ComponentsGroups.algorithms
  override val label = "Test algorithm"
  override val description = "Test algorithm"
  override val version = "1.0.0"

  override val modelBehaviorType = ModelBehaviorType.ModelConsumer

  val input1 = ComponentConnection(
    tag = typeTag[Any],
    label = "Vector",
    description = "Vector of attributes",
    group = ConnectionGroups.DATA)

  val input2 = ComponentConnection(
    tag = typeTag[Any],
    label = "Vector2",
    description = "Vector of attributes",
    group = ConnectionGroups.DATA)

  val output1 = ComponentConnection(
    tag = typeTag[String],
    label = "Anomaly",
    description = "Anomalous prediction output",
    group = ConnectionGroups.PREDICTION)

  val output2 = ComponentConnection(
    tag = typeTag[String],
    label = "Prediction",
    description = "Prediction output",
    group = ConnectionGroups.PREDICTION)


  override val inputTypes: ConnectionList = ConnectionList(input1, input2)
  override var outputTypes: ConnectionList = ConnectionList(output1, output2)

  override def materialize(env: ExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {
    return ArrayBuffer[DataWrapperBase](dsArr(0), dsArr(1))
  }
}