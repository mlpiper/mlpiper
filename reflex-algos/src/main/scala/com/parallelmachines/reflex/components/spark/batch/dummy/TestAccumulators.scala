package com.parallelmachines.reflex.components.spark.batch.dummy

import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.mlpiper.infrastructure._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._


class TestAccumulators extends SparkBatchComponent {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.general
  override val label: String = "Test accumulators"
  override val description: String = "Test different type of accumulators"
  override val version: String = "1.0.0"


  private val inOut = ComponentConnection(
    tag = typeTag[Any],
    label = "Any-type",
    description = "Does nothing but add artificial configured delay",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(inOut)
  override var outputTypes: ConnectionList = ConnectionList(inOut)


  val numIterations = ComponentAttribute("numIters", 100, "Number of iterations", "Set the number of total iterations", true)
  val sleepMsec = ComponentAttribute("sleepMsec", 100, "Sleep between iterations", "Time to sleep between each iteration", true)
  val longAccName = ComponentAttribute("longAccName", "table1.long-acc", "Long accumulator name", "Set long accumulator name", true)
  val doubleAccName = ComponentAttribute("doubleAccName", "table1.double-acc", "Double accumulator name", "Set double accumulator name", true)
  val collectionAccName = ComponentAttribute("collectionAccName", "table2.collection-acc", "Collection accumulator name", "Set collection accumulator name", true)
  attrPack.add(numIterations, sleepMsec, longAccName, doubleAccName, collectionAccName)


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

    logger.info(s"Num iterations: ${numIterations.value}")
    logger.info(s"Sleep between each iteration: ${sleepMsec.value} msec")
    logger.info(s"Long accumulator name: ${longAccName.value}")
    logger.info(s"Double accumulator name: ${doubleAccName.value}")
    logger.info(s"Collection accumulator name: ${collectionAccName.value}")

    val longAccum = env.longAccumulator(longAccName.value)
    val doubleAccum = env.doubleAccumulator(doubleAccName.value)
    val collectionAccum = env.collectionAccumulator[Long](collectionAccName.value)

    env.parallelize(1 to numIterations.value).foreach{ i =>
      longAccum.add(i)
      doubleAccum.add(i * 2.0)
      collectionAccum.add(i * 3)
    }

    return dsArr
  }
}
