package com.parallelmachines.reflex.components.spark.batch.dummy

import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent
import com.parallelmachines.reflex.components.{ComponentAttribute, ComponentAttributePack}
import com.parallelmachines.reflex.pipeline._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class TestLongLastingOperation extends SparkBatchComponent {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.general
  override val label: String = "Test long lasting operation"
  override val description: String = "Emulates long lasting operation"
  override val version: String = "1.0.0"
  override val engineType: ComputeEngineType.Value = ComputeEngineType.SparkBatch

  private val inOut = ComponentConnection(
    tag = typeTag[Any],
    label = "Any-type",
    description = "Does nothing but add artificial configured delay",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(inOut)
  override var outputTypes: ConnectionList = ConnectionList(inOut)

  val delay = ComponentAttribute("delay", 0, "Delay[msec]", "Emulate long lasting operation")
  attrPack.add(delay)

  /** Generate the DAG portion of the specific component and the specific engine
    *
    * @param env          Spark environment
    * @param dsArr        Array of DataStream[Any]
    * @param errPrefixStr Error prefix string to use when errors happens during the run of the DAG component
    * @return
    */
  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val rdd = dsArr(0).data[RDD[Any]]()
    rdd.count() // Make sure a job is submitted for execution

    logger.info(s"Going to sleep for ${delay.value} msec ...")
    Thread.sleep(delay.value)
    logger.info("Woke up from sleeping")

    return dsArr
  }
}
