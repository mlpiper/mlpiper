package org.mlpiper.infrastructure.rest

object RestApiName extends Enumeration {

  type RestApiName = Value
  val mlopsPrefix = Value("mlops")
  val uuid = Value("uuid")
  val models = Value("models")
  val stats = Value("stats")
  val metrics = Value("metrics")
  val download = Value("download")
  val events = Value("events")
}
