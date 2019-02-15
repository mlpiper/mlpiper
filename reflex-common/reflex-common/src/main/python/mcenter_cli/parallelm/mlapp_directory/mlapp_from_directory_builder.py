import json
import logging
import os
import pprint
from collections import OrderedDict

from parallelm.mcenter_objects.mcenter_model_formats import MCenterModelFormats
from parallelm.mcenter_objects.pipeline_types import PipelineTypes

from parallelm.mlapp_directory.mlapp_defs import MLAppPatternKeywords, MLAppKeywords, MLAppProfileKeywords, \
    GroupKeywords, ModelsKeywords, MLAppDirBuilderDefs, MLAppVersions, PipelineKeywords, ComponentDescriptionKeywords
from parallelm.mlapp_directory.component_description_helper import ComponentsDescriptionHelper
from parallelm.mlapp_directory.pipeline_json_helper import PipelineJSONHelper

# TODO: support uploading components if components directory is provided.
# TODO: use JSON schema validation
# TODO: fail if can not load models or models does not exists
# TODO: add download for mlapp that will generate a dir
# TODO: support uploading with prefix added for each pipeline/pattern/profile


class MLAppFromDirectoryBuilder(object):

    """
    Create an MLApp from a directory.
    The following parts will be created
    o Pipelines patterns
    o Pipeline profiles
    o MLApp pattern
    o MLApp profile

    o Models required by the MLApp will be uploaded if the model path is provided
    """
    def __init__(self, mclient, mlapp_dir):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._mclient = mclient
        self._mlapp_dir = mlapp_dir
        self._mlapp_info = None
        self._components_desc_helper = ComponentsDescriptionHelper(self._mclient.get_components())
        self._node_to_id = {}
        self._node_to_pipeline_id = {}
        self._prefix = ""
        self._postfix = ""
        self._pattern_id = None      # Will hold the pattern id once created
        self._pattern_info = None    # Will hold the pattern dictionary once created
        self._profile_id = None      # Will hold the profile id
        self._profile_info = None    # Will hold the profile info
        self._supported_mlapp_versions = [MLAppVersions.V2]

        self._load_mlapp_info()
        self._verify_and_load_pipeline_files()
        self._verify_mlapp()
        self._add_nodes_ids()
        self._fix_node_children_and_parents()
        self._fix_models()
        self._fix_pipelines_json()

    def _get_mlapp_nodes(self):
        return self._mlapp_info[MLAppKeywords.NODES]

    def _get_mlapp_node_info(self, node):
        return self._mlapp_info[MLAppKeywords.NODES][node]

    def _get_mlapp_node_info_by_id(self, node_id):
        for node_info in self._mlapp_info[MLAppKeywords.NODES].values():
            if node_info[MLAppKeywords.NODE_ID] == node_id:
                return node_info
        return None

    def _get_mlapp_name(self):
        return self._mlapp_info[MLAppKeywords.NAME]

    def _load_mlapp_info(self):
        mlapp_main_file = os.path.join(self._mlapp_dir, MLAppDirBuilderDefs.MLAPP_MAIN_FILE)
        self._logger.info("loading MLApp from: {}".format(mlapp_main_file))

        with open(mlapp_main_file) as owl_json_data:
            # Loading using OrderedDict to maintain the order of the nodes as in the file
            self._mlapp_info = json.load(owl_json_data, object_pairs_hook=OrderedDict)

        if MLAppKeywords.VERSION not in self._mlapp_info:
            raise Exception("Version keyword: {} is missing in MLApp definition")

        version = self._mlapp_info[MLAppKeywords.VERSION]
        if version not in self._supported_mlapp_versions:
            raise Exception("Version {} is not supported, supported versions are {}"
                            .format(version, self._supported_mlapp_versions))

    @staticmethod
    def _check_keyword_exists(keyword, info, error_fmt_string):
        if keyword not in info:
            raise Exception(error_fmt_string.format(keyword))
        else:
            return info[keyword]

    def _verify_pipeline(self, pipeline_json):
        pipe_error = "Error, keyword {} is missing in pipeline definition"

        pipe = self._check_keyword_exists(PipelineKeywords.PIPE_SECTION, pipeline_json, pipe_error)
        engine_type = self._check_keyword_exists(PipelineKeywords.ENGINE_TYPE, pipeline_json, pipe_error)
        version = self._check_keyword_exists(PipelineKeywords.VERSION, pipeline_json, pipe_error)
        if version not in self._supported_mlapp_versions:
            raise Exception("Pipeline version {} is not in supported list [{}]"
                            .format(version, self._supported_mlapp_versions))

        self._check_keyword_exists(PipelineKeywords.ENGINE_TYPE, pipeline_json, pipe_error)
        pipeline_type = self._check_keyword_exists(PipelineKeywords.PIPELINE_TYPE, pipeline_json, pipe_error)
        if pipeline_type not in PipelineTypes.list():
            raise Exception("{} should be one of the following: {}"
                            .format(PipelineKeywords.PIPELINE_TYPE, PipelineTypes.list()))

        for component_instance_name in pipe:
            component_name = pipe[component_instance_name][PipelineKeywords.COMPONENT_TYPE_NAME]
            comp_desc = self._components_desc_helper.get_component(engine_type, component_name)
            if comp_desc is None:
                raise Exception("Error getting component description for {}".format(component_name))

    def _verify_and_load_pipeline_files(self):
        """
        Loading the pipeline jsons from files and running basic verification
        :return:
        """
        for node in self._get_mlapp_nodes():
            node_info = self._get_mlapp_node_info(node)
            if MLAppKeywords.PIPELINE not in node_info:
                raise Exception("Keyword {} is missing in {} definition"
                                .format(MLAppKeywords.PIPELINE, MLAppKeywords.NODES))
            pipeline_file = node_info[MLAppKeywords.PIPELINE]
            if not os.path.isabs(pipeline_file):
                pipeline_file = os.path.join(self._mlapp_dir, pipeline_file)
            if not os.path.isfile(pipeline_file):
                raise Exception("Error: pipeline file is missing or is not a file: [{}]".format(pipeline_file))
            with open(pipeline_file) as pipeline_fp:
                pipeline_json = json.load(pipeline_fp, object_pairs_hook=OrderedDict)
                self._verify_pipeline(pipeline_json)
                node_info[MLAppKeywords.PIPELINE_JSON] = pipeline_json

    def _fix_pipeline_json(self, pipeline_json):
        """
        Fixing a single pipeline json to be like the way MCenter server is expecting it
        o Using components ids
        o Using a list of components instead of dict
        o In parent section using parent id and output id.

        :param pipeline_json:
        :return:
        """
        pipe = pipeline_json[PipelineKeywords.PIPE_SECTION]
        engine_type = pipeline_json[MLAppKeywords.PIPELINE_ENGINE_TYPE]
        new_pipe = []
        comp_name_to_id = {}
        comp_id_to_info = {}
        counter = 0

        # Setting id and fixing fields
        for component in pipe:
            self._logger.info("Fixing component: {}".format(pprint.pformat(component)))
            comp_info = pipe[component]
            comp_info[PipelineKeywords.COMPONENT_ID] = counter
            comp_name_to_id[component] = counter
            counter += 1
            comp_info[PipelineKeywords.COMPONENT_TYPE] = comp_info[PipelineKeywords.COMPONENT_TYPE_NAME]
            del(comp_info[PipelineKeywords.COMPONENT_TYPE_NAME])
            comp_info[PipelineKeywords.COMPONENT_NAME] = component
            comp_id_to_info[comp_info[PipelineKeywords.COMPONENT_ID]] = comp_info

        self._logger.info(pprint.pformat(comp_name_to_id))
        # Fixing parents field
        for component in pipe:
            comp_info = pipe[component]
            input_idx = 0
            for parent in comp_info[PipelineKeywords.COMPONENT_PARENTS]:
                # Fixing the parent id
                parent_name = parent[PipelineKeywords.PARENT_PARENT]
                parent_id = comp_name_to_id[parent_name]
                parent[PipelineKeywords.PARENT_PARENT] = parent_id
                parent_component_type = comp_id_to_info[parent_id][PipelineKeywords.COMPONENT_TYPE]
                # Fixing the parent output index
                if PipelineKeywords.PARENT_OUTPUT in parent:
                    output_label = parent[PipelineKeywords.PARENT_OUTPUT]

                    output_idx = self._components_desc_helper.get_component_output_index_by_label(
                        engine_type, parent_component_type, output_label)
                else:
                    # Will assume only 1 output idx=1 but will check the number of outputs of component
                    components_num_outputs = \
                        self._components_desc_helper.get_component_number_of_outputs(engine_type, parent_component_type)
                    if components_num_outputs > 1:
                        msg = ("Must provide output label for parent component [{}] since it has {} outputs "
                               + "(this happened in component [{}])")\
                            .format(parent_name, components_num_outputs, component)
                        raise Exception(msg)
                    output_idx = 0

                parent[PipelineKeywords.PARENT_OUTPUT] = output_idx

                # Setting the input according to order
                parent[PipelineKeywords.PARENT_INPUT] = input_idx
                input_idx += 1

        for component in pipe:
            comp_info = pipe[component]
            new_pipe.append(comp_info)
        pipeline_json[PipelineKeywords.PIPE_SECTION] = new_pipe

    def _fix_pipelines_json(self):
        """
        Convert the pipelines json to a backend format (using ids instead of names)
        :return:
        """

        for node in self._get_mlapp_nodes():
            node_info = self._get_mlapp_node_info(node)
            self._fix_pipeline_json(node_info[MLAppKeywords.PIPELINE_JSON])

    def _fix_models(self):
        """
        Going over the MLApp nodes and if detecting defaultModelPath or defaultModelName then doing the following
        In case of defaultModelName translating to modelId in case of defaultModelPath uploading the model and
        setting to modelId.

        Later methods of this class will use the defaultModelId in the profile or pattern
        :return:
        """
        for node in self._get_mlapp_nodes():
            node_info = self._get_mlapp_node_info(node)
            self._fix_node_models(node_info)

    def _model_name_to_id(self, model_name):

        models_info = self._mclient.get_models(timeout=50)
        for model_info in models_info:
           if model_info[ModelsKeywords.NAME] == model_name:
               return model_info[ModelsKeywords.ID]
        return None

    def _upload_model(self, model_path, model_format):
        """
        Helper to upload a model
        :param model_path: Path to model file
        :param model_format: Model format to use
        :return: model id of newly uploaded model
        """
        self._logger.info("Uploading default model from path: {}".format(model_path))

        if not os.path.isabs(model_path):
            model_path = os.path.abspath(os.path.join(self._mlapp_dir, model_path))

        if not os.path.exists(model_path):
            raise Exception("Model path [{}] does not exists".format(model_path))

        if model_format is None:
            raise Exception("Model format can not be None")
        elif model_format not in MCenterModelFormats.list():
            raise Exception('Wrong model format provided: {}, supported formats are {}'
                            .format(model_format, MCenterModelFormats.list()))

        model_id = self._mclient.post_model(model_path,
                                            os.path.basename(model_path),
                                            model_format)
        self._logger.info('Uploaded model, id: {}'.format(model_id))
        return model_id

    def _fix_node_models(self, node_info):

        # TODO: make sure either modelId or ModelName of ModelPath is used
        if MLAppKeywords.NODE_DEFAULT_MODEL_NAME in node_info:
            model_name = node_info[MLAppKeywords.NODE_DEFAULT_MODEL_NAME]
            model_id = self._model_name_to_id(model_name)
            node_info[MLAppKeywords.NODE_DEFAULT_MODEL_ID] = model_id
        elif MLAppKeywords.NODE_DEFAULT_MODEL_PATH in node_info:
            if MLAppKeywords.NODE_DEFAULT_MODEL_FORMAT not in node_info:
                raise Exception("Must provide {} if {} is used in MLApp"
                                .format(MLAppKeywords.NODE_DEFAULT_MODEL_FORMAT, MLAppKeywords.NODE_DEFAULT_MODEL_PATH))
            model_path = node_info[MLAppKeywords.NODE_DEFAULT_MODEL_PATH]
            model_format = node_info[MLAppKeywords.NODE_DEFAULT_MODEL_FORMAT]
            model_id = self._upload_model(model_path, model_format)
            node_info[MLAppKeywords.NODE_DEFAULT_MODEL_ID] = model_id

    def _create_pipeline_pattern(self, pipeline_json):

        pipe_helper = PipelineJSONHelper(pipeline_json)

        pipe_str = json.dumps(pipeline_json)
        payload = {
                    "name": pipe_helper.name,
                    "engineType": pipe_helper.engine_type,
                    "pipeline": pipe_str
        }
        self._logger.info("creating pipeline pattern: payload {}".format(pprint.pformat(payload)))
        pipeline_id = self._mclient.create_pipeline_pattern(payload)
        self._logger.info("Pipeline id: {}".format(pipeline_id))
        return pipeline_id

    def _create_pipelines_pattern(self):
        for node in self._get_mlapp_nodes():
            node_info = self._get_mlapp_node_info(node)
            pipeline_json = node_info[MLAppKeywords.PIPELINE_JSON]
            pipeline_id = self._create_pipeline_pattern(pipeline_json)
            node_info[MLAppKeywords.PIPELINE_ID] = pipeline_id

    def _create_pipeline_profile(self, node_info):
        """
        Assuming the pipeline json to be used for the profile as well
        :param node_info - Node dictionary
        :return: dict of agent id to profile id
        """

        pipeline_json = node_info[MLAppKeywords.PIPELINE_JSON]
        pattern_id = node_info[MLAppKeywords.PIPELINE_ID]

        pipe_helper = PipelineJSONHelper(pipeline_json)
        profile_name = pipe_helper.name + "-profile"
        pipe_str = json.dumps(pipeline_json)
        payload = {
                    "name": profile_name,
                    "engineType": pipe_helper.engine_type,
                    "pipeline": pipe_str,
                    "pipelinePatternId": pattern_id
        }

        if MLAppKeywords.NODE_DEFAULT_MODEL_ID in node_info:
            model_id = node_info[MLAppKeywords.NODE_DEFAULT_MODEL_ID]
            payload["defaultModelId"] = model_id

        self._logger.info("creating pipeline profile: payload {}".format(pprint.pformat(payload)))
        pipeline_profile_id = self._mclient.create_pipeline_profile(payload)

        return pipeline_profile_id

    def _create_pipelines_profile(self):
        for node in self._get_mlapp_nodes():
            node_info = self._get_mlapp_node_info(node)
            pipeline_profile_id = self._create_pipeline_profile(node_info)
            node_info[MLAppKeywords.PIPELINE_PROFILE_ID] = pipeline_profile_id

    def _add_nodes_ids(self):
        counter = 0
        for node in self._get_mlapp_nodes():
            node_info = self._get_mlapp_node_info(node)
            node_info[MLAppKeywords.NODE_ID] = counter
            self._node_to_id[node] = node_info[MLAppKeywords.NODE_ID]
            counter += 1

    def _fix_node_children_and_parents(self):
        """
        Fix the mlapp json dict to use children and parents like the backend format
        :return:
        """

        for node in self._get_mlapp_nodes():
            node_info = self._get_mlapp_node_info(node)
            parent = node_info[MLAppKeywords.NODE_PARENT]
            parent_len = len(parent)
            if parent_len > 1:
                raise Exception("Not handling MLApp nodes with more than one parent")
            elif parent_len == 1:
                parent_name = parent[0]
                parent_id = self._node_to_id[parent_name]
                node_info[MLAppPatternKeywords.NODE_PARENT] = str(parent_id)

                # Fixing parent to to have node as a child, only needed if we have a parent
                node_id = node_info[MLAppKeywords.NODE_ID]
                parent_node_info = self._get_mlapp_node_info(parent_name)

                if MLAppPatternKeywords.NODE_CHILDREN not in parent_node_info:
                    parent_node_info[MLAppPatternKeywords.NODE_CHILDREN] = str(node_id)
                else:
                    parent_node_info[MLAppPatternKeywords.NODE_CHILDREN] += ",{}".format(node_id)
            else:
                node_info[MLAppPatternKeywords.NODE_PARENT] = MLAppPatternKeywords.NODE_NO_PARENT_VAL

        # Adding children section if missing
        for node in self._get_mlapp_nodes():
            node_info = self._get_mlapp_node_info(node)
            if MLAppPatternKeywords.NODE_CHILDREN not in node_info:
                node_info[MLAppPatternKeywords.NODE_CHILDREN] = MLAppPatternKeywords.NODE_NO_CHILD_VAL

    def _create_node_pattern_info(self, node):
        """
        Create the pattern dict (json) for the node pattern
        :param node:
        :return:
        """
        node_info = self._get_mlapp_node_info(node)

        pipe_helper = PipelineJSONHelper(node_info[MLAppKeywords.PIPELINE_JSON])
        node_pattern_info = {
            MLAppPatternKeywords.NODE_ID: node_info[MLAppKeywords.NODE_ID],
            MLAppPatternKeywords.NODE_CHILDREN: node_info[MLAppPatternKeywords.NODE_CHILDREN],
            MLAppPatternKeywords.NODE_PARENT: node_info[MLAppPatternKeywords.NODE_PARENT],
            MLAppPatternKeywords.PIPELINE_PATTERN_ID: node_info[MLAppKeywords.PIPELINE_ID],
            MLAppPatternKeywords.CRON_SCHEDULE: node_info[MLAppKeywords.CRON_SCHEDULE],
            MLAppPatternKeywords.NODE_PIPELINE_TYPE: pipe_helper.pipeline_type,
            "pipelineMode": "offline",
        }

        return node_pattern_info

    def _create_mlapp_pattern(self):
        self._logger.info("Creating mlapp {} pattern".format(self._mlapp_info[MLAppKeywords.NAME]))
        nodes_info_list = []
        for node in self._get_mlapp_nodes():
            nodes_info_list.append(self._create_node_pattern_info(node))

        pattern_info = {
            MLAppPatternKeywords.NAME: self._get_mlapp_name(),
            MLAppPatternKeywords.NODES: nodes_info_list
        }
        self._logger.info("Pattern info\n{}".format(pprint.pformat(pattern_info)))

        # Generating the pattern in the server
        pattern_id = self._mclient.create_ion_pattern(pattern_info)
        self._logger.info(pattern_id)
        self._pattern_id = pattern_id
        self._pattern_info = pattern_info

    def _create_mlapp_profile(self):
        self._logger.info("Creating mlapp {} profile".format(self._mlapp_info[MLAppKeywords.NAME]))

        profile_info = self._pattern_info.copy()

        profile_info[MLAppProfileKeywords.IS_PROFILE] = True
        profile_info[MLAppProfileKeywords.PATTERN_ID] = self._pattern_id
        profile_info[MLAppProfileKeywords.MODEL_POLICY] = self._mlapp_info[MLAppKeywords.MODEL_POLICY]
        profile_info[MLAppProfileKeywords.GLOBAL_TRESHOLD] = self._mlapp_info[MLAppKeywords.GLOBAL_TRESHOLD]

        # Add pipelineEETuple for each node
        for profile_node_info in profile_info.get(MLAppProfileKeywords.NODES):
            node_id = profile_node_info[MLAppProfileKeywords.NODE_ID]
            mlapp_node_info = self._get_mlapp_node_info_by_id(node_id)

            pipeline_ee_info = {
                MLAppProfileKeywords.PIPELINE_EE_TUPLE_PIPELINE_PROFILE_ID: mlapp_node_info[MLAppKeywords.PIPELINE_PROFILE_ID]
            }
            profile_node_info[MLAppProfileKeywords.NODE_PIPELINE_EE_TUPLE] = pipeline_ee_info

        self._logger.info("MLApp profile {}".format(pprint.pformat(profile_info)))
        self._profile_id = self._mclient.create_ion_profile(profile_info)
        if not self._profile_id:
            raise Exception('Could not create profile for {}'.format(self._get_mlapp_name()))
        self._logger.info("MLApp profile id {} pattern id {}".format(self._profile_id, self._pattern_id))

    def _verify_mlapp(self):
        """
        Basic verification that the mlapp info is valid

        :return:
        """

        # Now only very basic validation
        if MLAppKeywords.NAME not in self._mlapp_info:
            raise Exception("{} section is mossing".format(MLAppKeywords.NAME))

        if MLAppKeywords.NODES not in self._mlapp_info:
            raise Exception("{} section is mossing".format(MLAppKeywords.NODES))

        for node in self._get_mlapp_nodes():
            node_info = self._get_mlapp_node_info(node)
            if MLAppKeywords.PIPELINE not in node_info:
                raise Exception("{} section is missing in node {}".format(MLAppKeywords.PIPELINE, node))

    def get_mlapp_name(self):
        return self._get_mlapp_name()

    def build(self):
        """
        Build an MLApp from directory
        :return: Void
        """

        self._create_pipelines_pattern()
        self._create_mlapp_pattern()
        self._create_pipelines_profile()
        self._create_mlapp_profile()
