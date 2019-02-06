package com.parallelmachines.reflex.factory

/**
  * List the supported tags in the components JSON description.
  * Tags not in this list will not cause failure of the pipeline
  */
object ComponentArgumentsTags extends Enumeration {
  type ComponentArgumentTag = Value

  val OutputModelPath: Value = Value("model_dir")
  val InputModelPath: Value = Value("input_model_path")
  val ModelVersion: Value = Value("model_version")
  val TFLogDir: Value = Value("tflog_dir")
  val PublicPort: Value = Value("public_port")
}
