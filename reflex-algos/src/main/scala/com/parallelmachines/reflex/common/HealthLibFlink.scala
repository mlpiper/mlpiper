package com.parallelmachines.reflex.common

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.streaming.api.scala.{DataStream, _}


private class HealthTypeFilterFunction(healthType: String) extends FilterFunction[HealthWrapper] {
  override def filter(healthWrapper: HealthWrapper): Boolean = {
    healthWrapper.nameEqualsOrContains(healthType)
  }
}

private class JsonToHealthWrapperMapFlink() extends MapFunction[String, HealthWrapper] {
  override def map(accumJson: String): HealthWrapper = {
    HealthWrapper(accumJson)
  }
}

class HealthLibFlink(enableCalculations: Boolean) extends HealthLib {

  override val enabled = enableCalculations

  var incomingHealthStream: DataStream[String] = _

  def setIncomingHealth(input: DataStream[String]): Unit = {
    incomingHealthStream = input
    hasIncomingHealth = true
  }

  override def init(): Unit = {
    if (hasIncomingHealth) {
      val keys = registeredComponents.keys.toList
      for (key <- keys) {
        val incomingHealthOfType = incomingHealthStream.map(new JsonToHealthWrapperMapFlink()).filter(new HealthTypeFilterFunction(key)).map(_.data)
        registeredComponents(key).asInstanceOf[HealthComponentFlink].setIncomingHealthStream(incomingHealthOfType)
      }
    }
  }
}