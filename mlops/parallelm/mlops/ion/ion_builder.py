from parallelm.mlops.common.string_ops import mask_passwords
from parallelm.mlops.ion.ion import ION, MLAppNode, Pipeline, EE, Agent, Policy
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.base_obj import BaseObj
from parallelm.mlops.constants import Constants
import logging
import pprint


class IONJsonConstants:
    ION_INFO_SECTION = "workflow"
    ION_COMP_SECTION = "workflow"
    ION_NAME_TAG = "name"
    ION_HEALTH_THRESHOLD_TAG = "healthThreshold"
    ION_GLOBAL_THRESHOLD_TAG = "globalThreshold"
    ION_CANARY_THRESHOLD_TAG = "canaryThreshold"

    PIPELINE_INSTANCES_SECTION = "pipelineInstances"
    PIPELINE_INSTANCES_TO_WFNODE_SECTION = "pipelineInstanceIdToWfNode"
    PIPELINE_NAME_TAG = "pipelineName"
    PIPELINE_ID_TAG = "pipelineId"
    PIPELINE_PROFILE_ID_TAG = "pipelineProfileId"
    PIPELINE_PATTERN_ID_TAG = "pipelinePatternId"
    PIPELINE_INSTANCE_ID_TAG = "pipelineInstanceId"
    PIPELINE_WORKFLOW_NODE_ID_TAG = "workflowNodeId"


class IONBuilder(BaseObj):
    def __init__(self):
        super(IONBuilder, self).__init__(__name__)

    def build_from_dict(self, json_dict):
        self._debug("Building {} from dict".format(Constants.ION_LITERAL))
        self._debug(mask_passwords(json_dict))
        ion = ION()
        ion.id = json_dict['id']
        ion.name = json_dict[IONJsonConstants.ION_INFO_SECTION][IONJsonConstants.ION_NAME_TAG]
        self._info("{} id [{}]".format(Constants.ION_LITERAL, json_dict['id']))

        ion_comps = json_dict[IONJsonConstants.ION_INFO_SECTION][IONJsonConstants.ION_COMP_SECTION]
        if len(ion_comps) < 1:
            msg = "{} components sections does not contain any component".format(
                Constants.ION_LITERAL)
            self._error(msg)
            raise MLOpsException(msg)

        for comp_dict in ion_comps:
            agent_set = {}

            wf_id = comp_dict["id"]
            pipeline_ee_tuple = comp_dict["pipelineEETuple"]
            ee = pipeline_ee_tuple["executionEnvironment"]
            agent_set[ee['agentId']] = pipeline_ee_tuple['pipelineProfileId']

            comp = MLAppNode(name=str(wf_id),
                           id=wf_id,
                           pipeline_pattern_id=str(comp_dict[
                                                        IONJsonConstants.PIPELINE_PATTERN_ID_TAG]),
                           pipeline_agent_set=agent_set,
                           ee_id=str(ee["id"]))
            ion.nodes.append(comp)
            ion.node_by_id[comp.id] = comp
            ion.node_by_name[comp.name] = comp

        pipeline_instances = json_dict[IONJsonConstants.PIPELINE_INSTANCES_SECTION]
        self._debug("pipelines number: {}".format(len(pipeline_instances)))
        for instance_data in pipeline_instances:
            comp_id = instance_data[IONJsonConstants.PIPELINE_WORKFLOW_NODE_ID_TAG]
            pipeline_instance_id = instance_data[IONJsonConstants.PIPELINE_INSTANCE_ID_TAG]
            if comp_id not in ion.node_by_id:
                raise MLOpsException("Error: could not find {} node id {} in pipeline "
                                     "instances".format(Constants.ION_LITERAL, comp_id))
            ion.node_by_id[comp_id].pipeline_instance_id = pipeline_instance_id

            pipeline = Pipeline(pipeline_name=instance_data["pipelineName"],
                                pipeline_id=instance_data["pipelineId"],
                                pipeline_type=instance_data["pipelineType"].upper())
            ion.pipelines.append(pipeline)
            # TODO maybe need to append?
            ion.pipeline_by_name[pipeline.pipeline_name] = pipeline
            ion.pipeline_by_id[pipeline.pipeline_id] = pipeline

            pipeline_id = instance_data['pipelineId']
            if pipeline_id not in ion.pipeline_by_id:
                raise MLOpsException("Error: could not find pipeline by id {}".format(pipeline_id))
            pipeline = ion.pipeline_by_id[pipeline_id]
            ion.pipelineInstance_to_pipeline[pipeline_instance_id] = pipeline
            if pipeline.pipeline_name not in ion.pipeline_to_pipelineInstances:
                ion.pipeline_to_pipelineInstances[pipeline.pipeline_name] = []
            ion.pipeline_to_pipelineInstances[pipeline.pipeline_name].append((comp_id, pipeline_instance_id))

        ion.policy = Policy(health_threshold=None, canary_threshold=None)

        ion._finalize()
        return ion
