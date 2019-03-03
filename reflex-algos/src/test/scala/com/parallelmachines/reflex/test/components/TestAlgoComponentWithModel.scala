package com.parallelmachines.reflex.test.components

import com.parallelmachines.reflex.components.flink.streaming.{FlinkStreamingComponent, StreamExecutionEnvironment}
import com.parallelmachines.reflex.components.flink.streaming.connectors.{EventSocketSource, ReflexNullConnector, ReflexNullSourceConnector}
import com.parallelmachines.reflex.components.spark.batch.algorithms.{ModelBehavior, ModelBehaviorType}
import com.parallelmachines.reflex.pipeline.{ComponentConnection, ComponentsGroups, ConnectionGroups, _}
import org.mlpiper.datastructures.PredictionOutput

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class TestAlgoComponentWithModel extends FlinkStreamingComponent with ModelBehavior {
  override val isSource = false
  override val group: String = ComponentsGroups.algorithms
  override val label = "Test Algo Component With Model"
  override val description = "Test Algo Component With Model"
  override val version = "1.0.0"
  override lazy val isVisible = false

  override val modelBehaviorType = ModelBehaviorType.ModelConsumer

  val input1 = ComponentConnection(
    tag = typeTag[Any],
    defaultComponentClass = Some(classOf[ReflexNullSourceConnector]),
    label = "Vector",
    description = "Vector of attributes",
    group = ConnectionGroups.DATA)

  val input2 = ComponentConnection(
    tag = typeTag[Any],
    defaultComponentClass = Some(classOf[EventSocketSource]),
    eventTypeInfo = Some(EventDescs.Model),
    label = "Model",
    description = "Model to use for predictions",
    group = ConnectionGroups.MODEL)

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

  override val inputTypes: ConnectionList = ConnectionList(input1, input2)
  override var outputTypes: ConnectionList = ConnectionList(output1, output2)

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    return ArrayBuffer[DataWrapperBase](dsArr(0), dsArr(0))
  }
}
