import logging
import json
import pprint

from parallelm.mcenter_objects.mcenter_exceptions import InitializationException
from parallelm.mcenter_objects.pipeline_base import PipelineBase


class PipelinePattern(PipelineBase):
    def __init__(self,
                 pipeline_info,
                 pipeline_mode,
                 pipeline_type,
                 pipeline_id,
                 node_A,
                 agent_name_A,
                 node_B,
                 agent_name_B,
                 mlapp_dir,
                 model_access,
                 user):

        PipelineBase.__init__(self,
                              pipeline_info,
                              pipeline_mode,
                              pipeline_type,
                              pipeline_id,
                              node_A,
                              agent_name_A,
                              node_B,
                              agent_name_B,
                              mlapp_dir,
                              model_access,
                              user)

    def create(self):
        if self.predefined:
            self._logger.info('Skip creating predefined pattern {} '.format(self.id))
            return self.id
        pipe_str = json.dumps(self._pipeline_json)
        payload = {"name": self.name, "engineType": self.engine_type,
                   "pipeline": pipe_str, "defaultModelId": self.default_model}
        self._logger.info("payload for pattern {}".format(pprint.pformat(payload)))
        self.id = self._mclient.create_pipeline_pattern(payload)
        if not self.id:
            raise InitializationException('Creation of pipeline pattern "{}" failed'.format(self.name))
        self._logger.info('Query pipeline pattern right '
                          'after creation {}'.format(self._mclient.get_pipeline_pattern(self.id)))
        return self.id
