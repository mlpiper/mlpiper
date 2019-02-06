package com.parallelmachines.reflex.test.components

import com.parallelmachines.reflex.pipeline.ComponentsGroups
import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.components.flink.streaming.algorithms.{ModelBehavior, ModelBehaviorType}
import com.parallelmachines.reflex.components.flink.streaming.connectors.{ReflexNullConnector, ReflexNullSourceConnector}
import com.parallelmachines.reflex.pipeline.{ComponentConnection, ConnectionGroups, _}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.scala.examples.common.algorithm.PredictionOutput
import org.apache.flink.streaming.scala.examples.common.performance.PerformanceMetricsHash

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class TestAlgoComponent extends FlinkStreamingComponent with ModelBehavior {
  override val isSource = false
  override val group: String = ComponentsGroups.algorithms
  override val label = "Test Algo Component"
  override val description = "Test Algo Component"
  override val version = "1.0.0"
  override lazy val isVisible = false

  override val modelBehaviorType = ModelBehaviorType.ModelConsumer

  val input1 = ComponentConnection(
    tag = typeTag[Any],
    defaultComponentClass = Some(classOf[ReflexNullSourceConnector]),
    label = "Vector",
    description = "Vector of attributes",
    group = ConnectionGroups.DATA)

  val output1 = ComponentConnection(
    tag = typeTag[String],
    defaultComponentClass = Some(classOf[ReflexNullConnector]),
    label = "Anomaly",
    description = "Anomalous prediction output",
    group = ConnectionGroups.PREDICTION)

  val output2 = ComponentConnection(
    tag = typeTag[PredictionOutput],
    defaultComponentClass = Some(classOf[ReflexNullConnector]),
    label = "Prediction",
    description = "Prediction output",
    group = ConnectionGroups.PREDICTION)

  val output3 = ComponentConnection(
    tag = typeTag[PerformanceMetricsHash],
    defaultComponentClass = Some(classOf[ReflexNullConnector]),
    label = "Statistics",
    description = "Performance metrics",
    group = ConnectionGroups.STATISTICS,
    isVisible = false)

  override val inputTypes: ConnectionList = ConnectionList(input1)
  override var outputTypes: ConnectionList = ConnectionList(output1, output2, output3)

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    return ArrayBuffer[DataWrapperBase](dsArr(0), dsArr(0), dsArr(0))
  }
}