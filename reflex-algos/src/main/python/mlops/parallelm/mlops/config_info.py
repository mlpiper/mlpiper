
import os
from parallelm.mlops.mlops_env_constants import MLOpsEnvConstants
from parallelm.mlops.constants import Constants


class ConfigInfo:
    def __init__(self):
        self.mlops_mode = None
        self.output_channel_type = None
        self.zk_host = None
        self.token = None
        self.ion_id = None
        self.ion_node_id = None
        self.mlops_server = None
        self.mlops_port = None
        self.model_id = None
        self.pipeline_id = None

    def __str__(self):
        s = ""
        s += "mode:     {}\n".format(self.mlops_mode)
        s += "output:   {}\n".format(self.output_channel_type)
        s += "zk:       {}\n".format(self.zk_host)
        s += "token:    {}\n".format(self.token)
        s += "{}:       {}\n".format(Constants.ION_LITERAL, self.ion_id)
        s += "node:     {}\n".format(self.ion_node_id)
        s += "server    {}\n".format(self.mlops_server)
        s += "port      {}\n".format(self.mlops_port)
        s += "model_id  {}\n".format(self.model_id)
        s += "pipeline: {}\n".format(self.pipeline_id)

        return s

    def _update_val_from_env(self, env_name, value):
        if value is None:
            return os.environ.get(env_name, value)
        else:
            return value

    def read_from_env(self):
        """
        Read configuration from environment variables.
        """
        self.mlops_mode = self._update_val_from_env(MLOpsEnvConstants.MLOPS_MODE, self.mlops_mode)
        self.output_channel_type = self._update_val_from_env(MLOpsEnvConstants.MLOPS_OUTPUT_CHANNEL,
                                                             self.output_channel_type)
        self.zk_host = self._update_val_from_env(MLOpsEnvConstants.MLOPS_ZK_HOST, self.zk_host)
        self.token = self._update_val_from_env(MLOpsEnvConstants.MLOPS_TOKEN, self.token)
        self.ion_id = self._update_val_from_env(MLOpsEnvConstants.MLOPS_ION_ID, self.ion_id)
        self.ion_node_id = self._update_val_from_env(MLOpsEnvConstants.MLOPS_ION_NODE_ID, self.ion_node_id)
        self.mlops_server = self._update_val_from_env(MLOpsEnvConstants.MLOPS_DATA_REST_SERVER, self.mlops_server)
        self.mlops_port = self._update_val_from_env(MLOpsEnvConstants.MLOPS_DATA_REST_PORT, self.mlops_port)
        self.model_id = self._update_val_from_env(MLOpsEnvConstants.MLOPS_MODEL_ID, self.model_id)
        self.pipeline_id = self._update_val_from_env(MLOpsEnvConstants.MLOPS_PIPELINE_ID, self.pipeline_id)
        return self

    def set_env(self):
        """
        Set configuration into environment variables.
        """
        os.environ[MLOpsEnvConstants.MLOPS_MODE] = self.mlops_mode
        os.environ[MLOpsEnvConstants.MLOPS_OUTPUT_CHANNEL] = self.output_channel_type

        # ZK might not be used (in attache mode for example)
        if self.zk_host:
            os.environ[MLOpsEnvConstants.MLOPS_ZK_HOST] = self.zk_host

        os.environ[MLOpsEnvConstants.MLOPS_TOKEN] = self.token
        os.environ[MLOpsEnvConstants.MLOPS_ION_ID] = self.ion_id
        os.environ[MLOpsEnvConstants.MLOPS_ION_NODE_ID] = self.ion_node_id
        os.environ[MLOpsEnvConstants.MLOPS_DATA_REST_SERVER] = self.mlops_server
        os.environ[MLOpsEnvConstants.MLOPS_DATA_REST_PORT] = self.mlops_port
        os.environ[MLOpsEnvConstants.MLOPS_PIPELINE_ID] = self.pipeline_id

        if self.model_id:
            os.environ[MLOpsEnvConstants.MLOPS_MODEL_ID] = self.model_id