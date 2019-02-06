package com.parallelmachines.reflex.components.spark.batch.connectors

import com.parallelmachines.reflex.components.{ComponentAttribute, ComponentAttributePack}
import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent
import com.parallelmachines.reflex.pipeline._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class FromStringCollection extends SparkBatchComponent {
  override val isSource: Boolean = true
  override val group: String = ComponentsGroups.dataGenerators
  override val label: String = "from-string-collection"
  override val description: String = "Enables to provide multiple strings, designed for testing purposes"
  override val version: String = "1.0.0"

  private val output1 = ComponentConnection(
    tag = typeTag[RDD[String]],
    label = "String",
    description = "String",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList.empty()
  override var outputTypes: ConnectionList = ConnectionList(output1)

  val samples = ComponentAttribute("samples", List[String](), "Samples", "User's provided list of strings")
  attrPack.add(samples)

  /** Generate the DAG portion of the specific component and the specific engine
    *
    * @param env          Flink environment
    * @param dsArr        Array of DataStream[Any]
    * @param errPrefixStr Error prefix string to use when errors happens during the run of the DAG component
    * @return
    */
  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {

    val data = env.parallelize(samples.value)

    ArrayBuffer(new DataWrapper(data))
  }
}
