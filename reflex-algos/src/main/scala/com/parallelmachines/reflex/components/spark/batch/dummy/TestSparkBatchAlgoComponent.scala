package com.parallelmachines.reflex.components.spark.batch.dummy

import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent
import com.parallelmachines.reflex.components.spark.batch.algorithms.{ModelBehavior, ModelBehaviorType}
import com.parallelmachines.reflex.components.spark.batch.connectors.ReflexNullConnector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.mlpiper.infrastructure._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class TestSparkBatchAlgoComponent extends SparkBatchComponent with ModelBehavior {
  override val isSource = false
  override val group: String = ComponentsGroups.algorithms
  override val label = "Test Spark Batch Algo Component"
  override val description = "Test Spark Batch Algo Component"
  override val version = "1.0.0"

  override val modelBehaviorType = ModelBehaviorType.ModelConsumer

  val input1 = ComponentConnection(
    tag = typeTag[RDD[String]],
    label = "Model",
    description = "Model",
    group = ConnectionGroups.MODEL)

  val input2 = ComponentConnection(
    tag = typeTag[RDD[String]],
    label = "Health",
    description = "Health",
    group = ConnectionGroups.STATISTICS)

  val output = ComponentConnection(
    tag = typeTag[String],
    defaultComponentClass = Some(classOf[ReflexNullConnector]),
    label = "Anomaly",
    description = "Anomalous prediction output",
    group = ConnectionGroups.PREDICTION)

  override val inputTypes: ConnectionList = ConnectionList(input1, input2)
  override var outputTypes: ConnectionList = ConnectionList(output)

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    return ArrayBuffer[DataWrapperBase](DataWrapper(dsArr(0)))
  }
}
