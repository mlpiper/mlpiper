package com.parallelmachines.mlops

import org.slf4j.LoggerFactory

object MLOpsEnvVariables {
  private val envVars = sys.env
  private val logger = LoggerFactory.getLogger(getClass)

  val agentRestHost = envVars.get(MLOpsEnvConstants.MLOPS_DATA_REST_SERVER.toString)
  val agentRestPort = envVars.get(MLOpsEnvConstants.MLOPS_DATA_REST_PORT.toString)
  val pipelineInstanceId = envVars.get(MLOpsEnvConstants.MLOPS_PIPELINE_ID.toString)
  val workflowInstanceId = envVars.get(MLOpsEnvConstants.MLOPS_ION_ID.toString)
  val modelId = envVars.get(MLOpsEnvConstants.MLOPS_MODEL_ID.toString)
  val token = envVars.get(MLOpsEnvConstants.MLOPS_TOKEN.toString)

  def init: Unit = {
    val ready = MLOpsEnvVariables.agentRestHost.isDefined && MLOpsEnvVariables.agentRestPort.isDefined
    if (!ready) {
      logger.error(s"Agent REST host/port env vars ${MLOpsEnvConstants.MLOPS_DATA_REST_SERVER.toString}/${MLOpsEnvConstants.MLOPS_DATA_REST_PORT.toString} are not set. All rest calls are ignored")
    }
  }
}
