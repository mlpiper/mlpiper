import os
import logging
import pprint
import json
from collections import OrderedDict


from parallelm.mlapp_directory.component_description_helper import ComponentsDescriptionHelper
from parallelm.mlapp_directory.mlapp_defs import MLAppKeywords, MLAppProfileKeywords, MLAppPatternKeywords, \
    MLAppDirBuilderDefs, PipelineKeywords, PipelineProfileKeywords, MLAppVersions, GroupKeywords


class DirectoryFromMLAppBuilder:
    def __init__(self, mclient, mlapp_name, mlapp_dest_dir):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._mclient = mclient
        self._components_desc_helper = ComponentsDescriptionHelper(self._mclient.get_components())
        self._mlapp_name = mlapp_name
        self._mlapp_dest_dir = mlapp_dest_dir
        self._pipelines_dir = None
        self._profile_id = None
        self._pattern_id = None
        self._pipeline_patterns_ids = []
        self._pipeline_profiles_ids = []
        self._pipeline_profiles_info = {}
        self._pipeline_patterns_info = {}
        self._pipelines_info = {}
        self._profile_info = None
        self._pattern_info = None
        self._mlapp_info = OrderedDict()
        self._group_id_to_name = {}

    def _get_groups_info(self):
        """
        Getting the group id and setting it in the mlapp
        :return:
        """
        groups_info = self._mclient.list_groups()
        self._logger.info(pprint.pformat(groups_info))
        for group_info in groups_info:
            group_name = group_info[GroupKeywords.NAME]
            group_id = group_info[GroupKeywords.ID]
            self._group_id_to_name[group_id] = group_name

    def _collect_all_info(self):
        self._get_groups_info()

        for profile_info in self._mclient.list_ion_profiles():
            self._logger.info("Profile part: [{}]".format(self._mlapp_name))
            self._logger.info(pprint.pformat(profile_info))

            profile_name = profile_info[MLAppProfileKeywords.NAME]
            if profile_name == self._mlapp_name:
                self._profile_info = profile_info
                self._pattern_name = profile_info[MLAppProfileKeywords.PATTERN_NAME]
                self._pattern_id = profile_info[MLAppProfileKeywords.PATTERN_ID]
                self._profile_id = profile_info[MLAppProfileKeywords.ID]
                self._logger.info("Found mlapp {} {}".format(profile_name, self._profile_id))

                for node_info in profile_info[MLAppProfileKeywords.NODES]:
                    node_id = node_info[MLAppProfileKeywords.NODE_ID]
                    pipeline_pattern_id = node_info[MLAppProfileKeywords.NODE_PIPELINE_PATTERN_ID]

                    agent_set = node_info[MLAppProfileKeywords.NODE_PIPELINE_AGENT_SET]
                    if len(agent_set) > 1:
                        raise Exception("Not supporting downlading an MLApp with AgentSet size > 1")

                    item = agent_set[0]
                    pipeline_profile_id = item[MLAppProfileKeywords.AGENT_SET_PIPELINE_PROFILE_ID]

                    self._pipelines_info[node_id] = {"profile": self._mclient.get_pipeline_profile(pipeline_profile_id),
                                                     "pattern": self._mclient.get_pipeline_pattern(pipeline_pattern_id)
                                                     }
                    # This item is in the node info by we need in at the pipeline level
                    self._pipelines_info[node_id]["profile"][PipelineKeywords.PIPELINE_TYPE] = \
                        node_info[MLAppProfileKeywords.NODE_PIPELINE_TYPE]
                return
        raise Exception("Could not find MLApp {}".format(self._mlapp_name))

    @staticmethod
    def _copy_key_val(dict_dest, dict_src, key_dest, key_src=None, exists=True):
        """
        Helper to copy a key/value from one dict to another
        :param dict_dest: Destination dict
        :param dict_src: Source dict
        :param key_dest: Destination key
        :param key_src: Src Key, if not provided destination key is used
        :param exists: If True, check if the key exists in source and only then copy it (but do not fail if does not
                       if key is not in source. Default is to fail if key is not in source
        """
        if key_src is None:
            key_src = key_dest
        if exists:
            if key_src in dict_src:
                dict_dest[key_dest] = dict_src[key_src]
        else:
            dict_dest[key_dest] = dict_src[key_src]

    def _fix_mlapp_node_parent(self):
        for node in self._mlapp_info[MLAppKeywords.NODES]:
            node_info = self._mlapp_info[MLAppKeywords.NODES][node]

            if node_info[MLAppKeywords.NODE_PARENT] in \
                (MLAppProfileKeywords.NODE_PARENT_MCENTER, MLAppPatternKeywords.NODE_NO_PARENT_VAL):
                node_info[MLAppKeywords.NODE_PARENT] = []
            else:
                node_info[MLAppKeywords.NODE_PARENT] = [node_info[MLAppKeywords.NODE_PARENT]]

    def _init_mlapp_info(self):
        self._mlapp_info[MLAppKeywords.VERSION] = MLAppVersions.V2
        self._mlapp_info[MLAppKeywords.NAME] = self._mlapp_name
        self._copy_key_val(self._mlapp_info, self._profile_info,
                           MLAppKeywords.MODEL_POLICY, MLAppProfileKeywords.MODEL_POLICY)
        self._copy_key_val(self._mlapp_info, self._profile_info,
                           MLAppKeywords.GLOBAL_TRESHOLD, MLAppProfileKeywords.GLOBAL_TRESHOLD)

        mlapp_nodes_info = OrderedDict()
        for profile_node_info in self._profile_info[MLAppProfileKeywords.NODES]:
            self._logger.info("Profile:\n{}".format(pprint.pformat(profile_node_info)))
            mlapp_node_info = {}
            node_id = profile_node_info[MLAppProfileKeywords.NODE_ID]

            # Things which are copied as is
            self._copy_key_val(mlapp_node_info, profile_node_info, MLAppKeywords.CRON_SCHEDULE, exists=True)
            self._copy_key_val(mlapp_node_info, profile_node_info, MLAppKeywords.NODE_PARENT)

            # FIX group
            group_name = self._group_id_to_name[profile_node_info[MLAppProfileKeywords.NODE_GROUP_ID]]
            mlapp_node_info[MLAppKeywords.GROUP_NAME] = group_name
            mlapp_nodes_info[node_id] = mlapp_node_info
        self._mlapp_info[MLAppKeywords.NODES] = mlapp_nodes_info
        self._fix_mlapp_node_parent()

    def _convert_pipeline_from_backend_to_v2(self, pipe):
        pipe_v2 = OrderedDict()
        pipe_v2[PipelineKeywords.VERSION] = MLAppVersions.V2
        self._copy_key_val(pipe_v2, pipe, PipelineKeywords.NAME)
        self._copy_key_val(pipe_v2, pipe, PipelineKeywords.ENGINE_TYPE)
        self._copy_key_val(pipe_v2, pipe, PipelineKeywords.PIPELINE_TYPE)
        component_list_backend = pipe[PipelineKeywords.PIPE_SECTION][PipelineKeywords.PIPE_SECTION]
        component_dict_v2 = {}

        comp_id_to_info = {}
        for comp_backend in component_list_backend:
            comp_id_to_info[comp_backend[PipelineKeywords.COMPONENT_ID]] = comp_backend

        for comp_backend in component_list_backend:
            self._logger.info("backend component:\n{}".format(pprint.pformat(comp_backend)))
            comp_v2 = OrderedDict()
            self._copy_key_val(comp_v2, comp_backend,
                               PipelineKeywords.COMPONENT_TYPE_NAME, PipelineKeywords.COMPONENT_TYPE)
            parents_v2 = []
            for parent in comp_backend[PipelineKeywords.COMPONENT_PARENTS]:
                parent_id = parent[PipelineKeywords.PARENT_PARENT]
                parent_name = comp_id_to_info[parent_id][PipelineKeywords.COMPONENT_NAME]

                output_idx = parent[PipelineKeywords.PARENT_OUTPUT]

                parent_component_type = comp_id_to_info[parent_id][PipelineKeywords.COMPONENT_TYPE]

                output_label = self._components_desc_helper.get_component_output_label_by_index(
                    pipe_v2[PipelineKeywords.ENGINE_TYPE], parent_component_type, output_idx)

                parent_v2 = OrderedDict()
                parent_v2[PipelineKeywords.PARENT_PARENT] = parent_name
                parent_v2[PipelineKeywords.PARENT_OUTPUT] = output_label
                parents_v2.append(parent_v2)
            comp_v2[PipelineKeywords.COMPONENT_PARENTS] = parents_v2
            self._copy_key_val(comp_v2, comp_backend, PipelineKeywords.ARGUMENTS)
            component_dict_v2[comp_backend[PipelineKeywords.NAME]] = comp_v2

        pipe_v2[PipelineKeywords.PIPE_SECTION] = component_dict_v2
        return pipe_v2

    def _build_pipeline_dict(self, profile_info, pattern_info):

        pipe = OrderedDict()
        self._copy_key_val(pipe, pattern_info, PipelineKeywords.NAME)
        self._copy_key_val(pipe, pattern_info, PipelineKeywords.ENGINE_TYPE)
        self._copy_key_val(pipe, profile_info, PipelineKeywords.PIPELINE_TYPE)
        pipeline_str = profile_info[PipelineProfileKeywords.PIPELINE_SECTION]
        pipeline_dict = json.loads(pipeline_str)
        pipe[PipelineKeywords.PIPE_SECTION] = pipeline_dict

        pipe = self._convert_pipeline_from_backend_to_v2(pipe)
        return pipe

    def _build_pipelines_dicts(self):

        for node_id in self._pipelines_info:
            self._logger.info("Building pipeine for node: {}".format(node_id))
            pipe_profile_info = self._pipelines_info[node_id]["profile"]
            pipe_pattern_info = self._pipelines_info[node_id]["pattern"]
            pipeline_info = self._build_pipeline_dict(pipe_profile_info, pipe_pattern_info)
            self._logger.info("Pipeline:\n{}".format(pprint.pformat(pipeline_info, width=120)))
            self._pipelines_info[node_id]["v2"] = pipeline_info

    def _gen_mlapp_dir_strucuture(self):
        """
        Generate the MLApp directory structure

        MLAPP-DIR -+
                   + components (Future - if requestedto download components)
                   + pipelines
                   + models     (Future - if requested to download model)
        """
        if os.path.exists(self._mlapp_dest_dir):
            raise Exception("Directory [{}] already exists".format(self._mlapp_dest_dir))
        os.mkdir(self._mlapp_dest_dir)
        pipelines_dir = os.path.join(self._mlapp_dest_dir, MLAppDirBuilderDefs.MLAPP_PIPELINES_DIR)
        os.mkdir(pipelines_dir)

    def _gen_pipelines_files(self):
        for node_id in self._pipelines_info:
            pipe_dict = self._pipelines_info[node_id]["v2"]
            pipe_file_name = pipe_dict[PipelineKeywords.NAME] + ".json"
            file_path_in_mlapp = os.path.join(".", MLAppDirBuilderDefs.MLAPP_PIPELINES_DIR, pipe_file_name)
            file_path = os.path.join(self._mlapp_dest_dir, MLAppDirBuilderDefs.MLAPP_PIPELINES_DIR, pipe_file_name)
            with open(file_path, "w") as pipe_fh:
                json.dump(pipe_dict, pipe_fh, indent=4)
            self._mlapp_info[MLAppKeywords.NODES][node_id][MLAppKeywords.PIPELINE] = file_path_in_mlapp

    def _gen_mlapp_file(self):
        mlapp_file = os.path.join(self._mlapp_dest_dir, MLAppDirBuilderDefs.MLAPP_MAIN_FILE)
        with open(mlapp_file, "w") as mlapp_fh:
            json.dump(self._mlapp_info, mlapp_fh, indent=4)

    def build(self):
        self._collect_all_info()
        self._logger.info("\n\n\n====== Profile")
        self._logger.info(pprint.pformat(self._profile_info))
        self._logger.info("\n\n\n====== Pattern")
        self._logger.info(pprint.pformat(self._pattern_info))
        for id, info in self._pipeline_profiles_info.items():
            self._logger.info(pprint.pformat(info))
        for id, info in self._pipeline_patterns_info.items():
            self._logger.info(pprint.pformat(info))

        self._init_mlapp_info()
        self._logger.info("MLApp:\n{}".format(pprint.pformat(self._mlapp_info)))

        self._build_pipelines_dicts()
        self._gen_mlapp_dir_strucuture()
        self._gen_pipelines_files()
        self._gen_mlapp_file()
