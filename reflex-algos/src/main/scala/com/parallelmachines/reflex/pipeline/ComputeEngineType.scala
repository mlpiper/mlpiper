package com.parallelmachines.reflex.pipeline

object ComputeEngineType extends Enumeration {
  type ComputeEngineType = Value
  val FlinkStreaming = Value("FlinkStreaming")
  val SparkBatch = Value("SparkBatch")
  val PySpark = Value("PySpark")
  val Tensorflow = Value("Tensorflow")
  val Python = Value("Python")
  val RestModelServing = Value("RestModelServing")
}

object ExternalDirEngines {
  val externalEngineList: List[ComputeEngineType.Value] =
    List[ComputeEngineType.Value](ComputeEngineType.PySpark, ComputeEngineType.Tensorflow, ComputeEngineType.Python,
      ComputeEngineType.RestModelServing)
  val externalEngineListString: List[String] =
    externalEngineList.map(_.toString)

  def isExternalDirEngine(computeEngineType: ComputeEngineType.Value): Boolean = {
    externalEngineList.contains(computeEngineType)
  }
  def isExternalDirEngine(computeEngineType: String): Boolean = {
    externalEngineListString.contains(computeEngineType)
  }
}
