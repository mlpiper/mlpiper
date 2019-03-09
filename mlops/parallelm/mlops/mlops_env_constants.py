
# TODO: this should be generated at the reflex-common level
# Protobuf is not suitable for this but we need to find another tool


class MLOpsEnvConstants:
    # should be :class:`MLOpsMode`
    MLOPS_MODE = "PM_MLOPS_MODE"

    # This value should be "python", "pyspark", etc. See :class:`MLOpsMode` for details.
    MLOPS_OUTPUT_CHANNEL = "PM_MLOPS_OUTPUT_CHANNEL"

    # When running in stand_alone mode, output stats to this file.
    PM_MLOPS_FILE = "PM_MLOPS_FILE"

    # When running pyspark, construct REST endpoint here to export statistics.
    REST_SERVER_PORT = "PM_MLOPS_REST_SERVER_PORT"
    REST_SERVER_WAIT_FOR_EXIT = "PM_REST_SERVER_WAIT_FOR_EXIT"

    MLOPS_TIMESERIES_DB_HOST = "PM_TIMESERIES_DB_HOST"
    MLOPS_TIMESERIES_DB_PORT = "PM_TIMESERIES_DB_PORT"

    MLOPS_ION_ID = "PM_WORKFLOW_INSTANCE_ID"
    MLOPS_ION_NODE_ID = "PM_WORKFLOW_NODE_ID"
    MLOPS_PIPELINE_ID = "PM_PIPELINE_INSTANCE_ID"

    MLOPS_ZK_HOST = "PM_ZK_HOST"
    MLOPS_TOKEN = "PM_MLOPS_USER_TOKEN"

    # Where send ML objects, such as models. This is the agent in the ParallelM service.
    MLOPS_ML_OBJECT_SERVER_HOST = "ML_OBJECT_SERVER_HOST"
    MLOPS_ML_OBJECT_SERVER_PORT = "ML_OBJECT_SERVER_PORT"

    # Holds the agent host name, as registered in ZooKeeper. It is set by the ECO agent on startup.
    MLOPS_AGENT_PUBLIC_ADDRESS = "AGENT_PUBLIC_ADDRESS"

    # REST endpoint for the ECO server
    MLOPS_DATA_REST_SERVER = "PM_MLOPS_DATA_REST_SERVER"
    MLOPS_DATA_REST_PORT = "PM_MLOPS_DATA_REST_PORT"

    # Contains the model ID of the current model for inference pipeline (when the pipeline is running in batch mode)
    MLOPS_MODEL_ID = "PM_MLOPS_MODEL_ID"