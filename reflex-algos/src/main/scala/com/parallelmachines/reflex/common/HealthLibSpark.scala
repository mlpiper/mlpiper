package com.parallelmachines.reflex.common

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object JsonToHealthWrapperMapSpark {
  def map(accumJson: String): HealthWrapper = {
    HealthWrapper(accumJson)
  }
}

object SparkEventTypeFilterFunction {
  def filter(healthWrapper: HealthWrapper, healthType: String): Boolean = {
    healthWrapper.nameEqualsOrContains(healthType)
  }
}

class HealthLibSpark(enableCalculations: Boolean) extends HealthLib {
  override val enabled = enableCalculations

  private var incomingHealthStream: RDD[String] = _
  private var sparkContext: SparkContext = _
  private var rddOfDenseVector: RDD[DenseVector[Double]] = _
  private var dfOfDenseVector: DataFrame = _

  def setIncomingHealth(input: RDD[String]): Unit = {
    incomingHealthStream = input
    hasIncomingHealth = true
  }

  def setContext(sc: SparkContext): Unit = {
    sparkContext = sc
  }

  def setRddOfDenseVector(_rddOfDenseVector: RDD[DenseVector[Double]]): Unit = {
    rddOfDenseVector = _rddOfDenseVector
  }

  def setDfOfDenseVector(_dfOfDenseVector: DataFrame): Unit = {
    dfOfDenseVector = _dfOfDenseVector
  }

  override def init(): Unit = {
    registeredComponents.foreach(x => x._2.asInstanceOf[HealthComponentSpark].setContext(sc = sparkContext))
    registeredComponents.foreach(x => x._2.asInstanceOf[HealthComponentSpark].setDfOfDenseVector(_dfOfDenseVector = dfOfDenseVector))
    registeredComponents.foreach(x => x._2.asInstanceOf[HealthComponentSpark].setRddOfDenseVector(_rddOfDenseVector= rddOfDenseVector))
    if (hasIncomingHealth) {
      val keys = registeredComponents.keys.toList
      for (key <- keys) {
        val incomingHealthOfType = incomingHealthStream.map(x => JsonToHealthWrapperMapSpark.map(x)).filter(SparkEventTypeFilterFunction.filter(_, key)).map(_.data)
        registeredComponents(key).asInstanceOf[HealthComponentSpark].setIncomingHealthStream(incomingHealthOfType)
      }
    }
  }
}