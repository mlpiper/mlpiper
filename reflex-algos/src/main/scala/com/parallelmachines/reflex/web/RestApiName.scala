package com.parallelmachines.reflex.web

object RestApiName extends Enumeration {

  type ModelFormat = Value
  val mlopsPrefix = Value("mlops")
  val uuid = Value("uuid")
  val models = Value("models")
  val stats = Value("stats")
  val modelStats = Value("modelStats")
  val download = Value("download")
  val events = Value("events")
}
