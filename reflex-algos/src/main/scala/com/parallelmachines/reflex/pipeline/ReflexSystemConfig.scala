package com.parallelmachines.reflex.pipeline

object ReflexSystemConfig {
  def apply(): ReflexSystemConfig =
    new ReflexSystemConfig("", 0, "", "", "", None, None, None, None, None, None, None, None, None, None, None, None)

  val EnableHealthKey = "enableHealth"
}

case class ReflexSystemConfig(var statsDBHost: String,
                              var statsDBPort: Int,
                              var workflowInstanceId: String,
                              var statsMeasurementID: String,
                              var modelFileSinkPath: String,
                              var modelFileSourcePath: Option[String],
                              var healthStatFilePath: Option[String],
                              var healthStatSocketSourcePort: Option[Int],
                              var mlObjectSocketHost: Option[String],
                              var mlObjectSocketSourcePort: Option[Int],
                              var mlObjectSocketSinkPort: Option[Int],
                              var canaryConfig: Option[CanaryConfig],
                              var socketSourceHost: Option[String],
                              var socketSourcePort: Option[Int],
                              var socketSinkHost: Option[String],
                              var socketSinkPort: Option[Int],
                              var enableHealth: Option[Boolean])

//TODO: change CanaryConfig to be list when we decide to compare many pipelines
case class CanaryConfig(var canaryLabel1: Option[String], var canaryLabel2: Option[String])