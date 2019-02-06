package com.parallelmachines.reflex.components.spark.batch.dummy

import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent
import com.parallelmachines.reflex.pipeline._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class TestStdoutSink extends SparkBatchComponent {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.general
  override val label: String = "Dummy sink"
  override val description: String = "Dummy sink"
  override val version: String = "1.0.0"
  override val engineType: ComputeEngineType.Value = ComputeEngineType.SparkBatch

  private val input1 = ComponentConnection(
    tag = typeTag[Any],
    label = "Any-type",
    description = "Prints to the standard output",
    group = ConnectionGroups.OTHER)

  override val inputTypes: ConnectionList = ConnectionList(input1)
  override var outputTypes: ConnectionList = ConnectionList.empty()

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val rdd = dsArr(0).data[RDD[Any]]()
    rdd.collect().foreach(println)

    ArrayBuffer[DataWrapperBase]()
  }
}