package org.mlpiper.infrastructure

object ComputeEngineType extends Enumeration {
  type ComputeEngineType = Value
  val FlinkStreaming = Value("FlinkStreaming")
  val SparkBatch = Value("SparkBatch")
  val PySpark = Value("PySpark")
  val Generic = Value("Generic")
  val RestModelServing = Value("RestModelServing")
  val SageMaker = Value("SageMaker")
}

object ExternalDirEngines {
  val externalEngineList: List[ComputeEngineType.Value] =
    List[ComputeEngineType.Value](ComputeEngineType.PySpark, ComputeEngineType.Generic,
      ComputeEngineType.RestModelServing, ComputeEngineType.SageMaker)
  val externalEngineListString: List[String] =
    externalEngineList.map(_.toString)

  def isExternalDirEngine(computeEngineType: ComputeEngineType.Value): Boolean = {
    externalEngineList.contains(computeEngineType)
  }
  def isExternalDirEngine(computeEngineType: String): Boolean = {
    externalEngineListString.contains(computeEngineType)
  }
}
