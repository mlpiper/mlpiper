package com.parallelmachines.reflex.pipeline

object ConnectionGroups extends Enumeration {
  type ConnectionGroups = Value
  val DATA = Value("data")
  val MODEL = Value("model")
  val PREDICTION = Value("prediction")
  val STATISTICS = Value("statistics")
  val OTHER = Value("other")
}
