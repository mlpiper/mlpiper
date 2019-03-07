import logging
import json
import pprint

from parallelm.mcenter_objects.mcenter_exceptions import InitializationException
from parallelm.mcenter_objects.pipeline_base import PipelineBase


class PipelineProfile(PipelineBase):
    def __init__(self,
                 pipeline_info,
                 pipeline_mode, pipeline_type,
                 pipeline_id,
                 node_A, agent_name_A,
                 node_B, agent_name_B,
                 mlapp_dir,
                 model_access,
                 agent_name,
                 pipeline_pattern_id,
                 user):
        self.pipeline_pattern_id = pipeline_pattern_id
        self.agent_name = agent_name

        PipelineBase.__init__(self,
                              pipeline_info,
                              pipeline_mode, pipeline_type,
                              pipeline_id,
                              node_A, agent_name_A,
                              node_B, agent_name_B,
                              mlapp_dir,
                              model_access,
                              user)

    def create(self):
        if self.pipeline_pattern_id is None:
            raise InitializationException('Creation of pipeline profile "{}" failed: A '
                                          'profile requires a pipeline pattern '
                                          'id'.format(self.name))
        if self.predefined:
            self._logger.info('Skip creating predefined profile {} '.format(self.id))
            return self.id
        # conservatively renaming all profiles, be it old or new format
        # and creating one per agent
        self.name = self.name + "-profile-" + self.agent_name
        self._pipeline_json['name'] = self.name

        pipe_str = json.dumps(self._pipeline_json)
        payload = {"name": self.name, "engineType": self.engine_type,
                   "pipeline": pipe_str, "defaultModelId": self.default_model,
                   "pipelinePatternId": self.pipeline_pattern_id}
        self._logger.info("payload for profile: {}".format(pprint.pformat(payload)))
        self.id = self._mclient.create_pipeline_profile(payload)
        if not self.id:
            raise InitializationException('Creation of pipeline profile "{}" failed'.format(self.name))
