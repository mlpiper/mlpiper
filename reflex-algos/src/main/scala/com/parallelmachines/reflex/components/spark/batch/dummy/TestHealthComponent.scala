package com.parallelmachines.reflex.components.spark.batch.dummy

import breeze.linalg.DenseVector
import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent
import com.parallelmachines.reflex.components.spark.batch.connectors.EventSocketSource
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.mlpiper.infrastructure._
import org.mlpiper.stat.healthlib.{HealthComponentSpark, HealthLibSpark}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._


class TestHealthForSpark(idName: String) extends HealthComponentSpark {

  var incomingHealthStream: RDD[String] = _

  override def getName(): String = idName

  override def generateHealth(): Unit = {}

  override def generateHealthAndCompare(): Unit = {}

  override def setIncomingHealthStream(input: RDD[String]): Unit = {
    incomingHealthStream = input
  }

  override def setContext(sc: SparkContext): Unit = {}

  def setRddOfDenseVector(_rddOfDenseVector: RDD[DenseVector[Double]]): Unit = {}

  def setDfOfDenseVector(_dfOfDenseVector: DataFrame): Unit = {}

  def getIncomingHealthStream(): RDD[String] = incomingHealthStream
}

class TestHealthComponent extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.algorithms
  override val label: String = "TestHealthComponent"
  override val description: String = "TestHealthComponent"
  override val version: String = "1.0.0"

  val input1 = ComponentConnection(
    tag = typeTag[RDD[String]],
    defaultComponentClass = Some(classOf[EventSocketSource]),
    eventTypeInfo = Some(EventDescs.Model),
    label = "Model",
    description = "Model used to detect anomalies",
    group = ConnectionGroups.MODEL)

  val input2 = ComponentConnection(
    tag = typeTag[RDD[String]],
    defaultComponentClass = Some(classOf[EventSocketSource]),
    eventTypeInfo = Some(EventDescs.MLHealthModel),
    label = "Contender Health Stat",
    description = "ML Health Stream",
    group = ConnectionGroups.STATISTICS)

  val output = ComponentConnection(
    tag = typeTag[RDD[String]],
    label = "Test output",
    description = "Test output",
    group = ConnectionGroups.PREDICTION)

  override val inputTypes: ConnectionList = ConnectionList(input1, input2)
  override var outputTypes: ConnectionList = ConnectionList(output)

  private var enableHealth = true

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)
    enableHealth = paramMap.getOrElse(ReflexSystemConfig.EnableHealthKey, true).asInstanceOf[Boolean]
  }

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val healthLib = new HealthLibSpark(enableHealth)
    healthLib.setContext(env)
    healthLib.setIncomingHealth(dsArr(1).data[RDD[String]])

    val histStat = new TestHealthForSpark("TestHealthForSpark")

    healthLib.addComponent(histStat)
    healthLib.generateHealth()

    /**
      * Only for test case.
      * If Health is enabled, incoming health stream will be set.
      * If not, assign empty dataset.
      **/
    var ret = histStat.getIncomingHealthStream()
    if (ret == null) {
      ret = env.emptyRDD[String]
    }

    ArrayBuffer(DataWrapper(ret))
  }
}
