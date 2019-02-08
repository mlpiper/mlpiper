package com.parallelmachines.reflex.test.components

import breeze.linalg.DenseVector
import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.components.flink.streaming.connectors.{ReflexNullConnector, ReflexNullSourceConnector}
import com.parallelmachines.reflex.pipeline.{ComponentConnection, ComponentsGroups, ConnectionGroups, _}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.scala.examples.flink.utils.functions.performance.PerformanceMetricsHash
import org.mlpiper.datastructures.PredictionOutput

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

/**
  * Test component which simulates Algorithm component but without Model Behavior.
  *
  * Reason:
  * Model behavior would trigger addition of ModelReceiverComponent, which adds EventSocketSource.
  * Current test uses Text File as a source.
  *
  * Having two sources in MiniCluster Tests will fail to stop a job. Reason unknown.
  *
  **/
class TestAlgoComponentWithoutModelBehavior extends FlinkStreamingComponent {
  override val isSource = false
  override val group: String = ComponentsGroups.algorithms
  override val label = "Test Algo Component"
  override val description = "Test Algo Component 2"
  override val version = "1.0.0"

  val addPredictionOutput = ComponentAttribute("addPredictionOutput", false, "addPredictionOutput", "addPredictionOutput", optional = true)
  attrPack.add(addPredictionOutput)

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
    group = ConnectionGroups.STATISTICS)

  override val inputTypes: ConnectionList = ConnectionList(input1)
  override var outputTypes: ConnectionList = ConnectionList(output1, output2, output3)

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    var predictionOutput: DataStream[PredictionOutput] = null
    if (addPredictionOutput.value) {
      predictionOutput = dsArr(0).data[DataStream[DenseVector[Double]]].map(x => new PredictionOutput(Some(x(0)), x, None))
    } else {
      predictionOutput = dsArr(0).data[DataStream[PredictionOutput]]
    }

    return ArrayBuffer[DataWrapperBase](dsArr(0), DataWrapper(predictionOutput), dsArr(0))
  }
}
