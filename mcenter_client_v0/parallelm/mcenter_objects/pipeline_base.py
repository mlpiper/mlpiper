import logging
import json
import os

from parallelm.mcenter_objects import mcenter_defs
from parallelm.mcenter_objects.mcenter_exceptions import InitializationException
from parallelm.mcenter_objects.mcenter_model_formats import MCenterModelFormats


class PipelineBase:
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
                 mclient):
        """
        Handles pipelines and their data
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._mclient = mclient
        self.pipeline_mode = pipeline_mode
        self.pipeline_type = pipeline_type
        self.pipeline_info = pipeline_info
        self._pipeline_json = None
        self.name = None
        self.predefined = False

        if pipeline_id == mcenter_defs.INTERNAL_PREDEFINED_PIPELINE:
            # for predefined pipelines, we query system pipeline and get info from there
            self.id = mcenter_defs.INTERNAL_PREDEFINED_PIPELINE
            query_pipeline = self._mclient.get_pipeline_pattern(self.id)
            self._pipeline_json = json.loads(query_pipeline.get('pipeline'))
            self.predefined = True
        elif pipeline_id == 'comparatorPipeline':
            query_pipeline = self.find_comparator_pipeline()
            self._pipeline_json = json.loads(query_pipeline.get('pipeline'))
            self.predefined = True
        else:
            # here we create pipeline according to given template
            self.load_data(pipeline_info)
            self.process_pipeline_info()
            self.process_comparator(node_A, agent_name_A, node_B, agent_name_B)

        self.default_model = self._default_model_id(pipeline_info, model_access, mlapp_dir)

    def _default_model_id(self, pipeline_info, model_access, owl_config_dir_path):
        default_model_name = pipeline_info.get('default_model_name')
        default_model_path = pipeline_info.get('default_model_path')
        default_model_format = pipeline_info.get('default_model_format')
        known_formats = MCenterModelFormats.supported_model_formats()

        if default_model_name is not None and default_model_path is not None:
            msg = "There are two contradictory attributes in pipeline section: " \
                  "'default_model_name' & " \
                  "'default_model_path'! Remove one of them! workflow_ref_id: {}" \
                .format(pipeline_info.get('workflow_ref_id'))
            self._logger.error(msg)
            raise InitializationException(msg)

        if default_model_name:
            return model_access.model_id(default_model_name)
        elif default_model_path:
            self._logger.info("Requested to upload default model from path: {}".format(default_model_path))
            if not os.path.isabs(default_model_path):
                default_model_path = os.path.abspath(os.path.join(owl_config_dir_path,
                                                                  default_model_path))
            if default_model_format is None:
                default_model_format = MCenterModelFormats.DEFAULT_MODEL_FORMAT
                self._logger.info("No default model format explicitly specified, "
                                  "setting to {}".format(default_model_format))

            elif default_model_format not in known_formats:
                raise Exception('Wrong model format listed in pipeline info: {} '
                                'should be one of {}'.format(default_model_format, known_formats))

            if os.path.exists(default_model_path):
                default_model_id = self._mclient.post_model(default_model_path,
                                                            os.path.basename(default_model_path),
                                                            default_model_format)
                self._logger.info('Uploading default model, returned: {}'.format(default_model_id))
                return default_model_id
            else:
                msg = "Error: model path {} does not exist".format(default_model_path)
                self._logger.error(msg)
                raise InitializationException(msg)

        return ""

    @property
    def topic_name(self):
        topic = "defaultTopic"
        data = self.pipeline_info.get('data')
        if data:
            topic = data.get('topic')
        return topic

    @property
    def engine_type(self):
        return self._pipeline_json['engineType']

    def load_data(self, pipeline_info):
        if 'profile_path' in pipeline_info:
            # presence of a profile path means it is a profile - cant be a pipeline pattern or
            # pipeline template from old format
            p_file = pipeline_info.get('profile_path')
        elif 'pipeline_pattern' in pipeline_info:
            # no profile path, but has pattern, => new format, pipeline pattern
            p_file = pipeline_info.get('pipeline_pattern')
        else:
            raise Exception('"profile_path" and "profile_path" should be '
                            'defined for pipeline {}, entries not found'.format(pipeline_info))

        if not os.path.isfile(p_file):
            raise Exception("File to load pipeline from not found: " + p_file)
        else:
            with open(p_file) as f:
                try:
                    self._pipeline_json = json.load(f)
                    self.name = self._pipeline_json['name']
                except ValueError as e:
                    self._logger.error("Invalid JSON : %s %s " % (f, e))
                    raise Exception("Invalid JSON : %s %s " % (f, e))

    def process_pipeline_info(self):
        system_config_key = "systemConfig"
        if system_config_key in self._pipeline_json:
            del self._pipeline_json[system_config_key]

    def dump(self):
        with open('data.txt', 'w') as f:
            json.dump(self._pipeline_json, f)

    def print_self(self):
        self._logger.info('Pipeline id: ' + str(self.id))

    def find_comparator_pipeline(self, name):
        """
        This is temporary until
        we have a better way of
        finding exact pipeline
        :return:
        """
        pipeline_info = {}
        all_pipelines = self._mclient.list_pipeline_patterns()
        pipelines = [x for x in all_pipelines if x['id'].startswith(name)]
        self._logger.info('Found pipelines with id {}: {}'.format(name, pipelines))
        if pipelines:
            pipeline_info = pipelines[0]
        return pipeline_info

    def process_comparator(self, node_a, agent_name_a, node_b, agent_name_b):
        if node_a is None or \
                agent_name_a is None or \
                node_b is None or \
                agent_name_b is None:
            return
        pipe_list = self._pipeline_json['pipe']
        if len(pipe_list) == 1:
            pipe = pipe_list[0]
            pipe['arguments']['nodeA'] = node_a
            pipe['arguments']['nodeB'] = node_b
            pipe['arguments']['agentA'] = agent_name_a
            pipe['arguments']['agentB'] = agent_name_b
