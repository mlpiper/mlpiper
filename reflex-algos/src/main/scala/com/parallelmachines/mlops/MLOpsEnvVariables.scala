package com.parallelmachines.mlops

object MLOpsEnvVariables {
  private val envVars = sys.env

  val agentRestHost = envVars.get(MLOpsEnvConstants.MLOPS_DATA_REST_SERVER.toString)
  val agentRestPort = envVars.get(MLOpsEnvConstants.MLOPS_DATA_REST_PORT.toString)
  val pipelineInstanceId = envVars.get(MLOpsEnvConstants.MLOPS_PIPELINE_ID.toString)
  val workflowInstanceId = envVars.get(MLOpsEnvConstants.MLOPS_ION_ID.toString)
  val modelId = envVars.get(MLOpsEnvConstants.MLOPS_MODEL_ID.toString)
  val token = envVars.get(MLOpsEnvConstants.MLOPS_TOKEN.toString)
}
