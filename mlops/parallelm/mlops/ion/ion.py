"""
Context objects give information about the IONs, pipelines, etc. that this
component is running in.
"""

from parallelm.mlops.constants import Constants


class ION(object):
    def __init__(self):
        self.id = None
        self.template_id = None
        self.name = None
        self.nodes = []
        self.node_by_id = {}
        self.node_by_name = {}
        self.node_by_pipe_inst_id = {}
        self.policy = None
        self.pipelines = []
        self.pipeline_by_name = {}
        self.pipeline_by_id = {}
        self.pipelineInstance_to_pipeline = {}
        self.pipeline_to_pipelineInstances = {}

    def _finalize(self):
        """
        Building access dicts once all components are in place
        :return:
        """

        for node in self.nodes:
            self.node_by_id[node.id] = node

        for node in self.nodes:
            self.node_by_pipe_inst_id[node.pipeline_instance_id] = node

    def __str__(self):
        s = "{}:\n".format(Constants.ION_LITERAL)
        s += "name: {}\nid: {}\ntemplate_id: {}\n".format(self.name, self.id, self.template_id)
        s += "Nodes:"
        for node in self.nodes:
            s += "\tnode name: {}   id: {}\n".format(node.name, node.id)

        s += "Pipelines:\n"
        for pipe in self.pipelines:
            s += "\tpipe name: {}   id: {}\n".format(pipe.pipeline_name, pipe.pipeline_id)

        return s


class Policy(object):
    def __init__(self, health_threshold=None, canary_threshold=None):
        self.set_thresholds(health_threshold, canary_threshold)

    def set_thresholds(self, health_threshold=None, canary_threshold=None):
        self.health_threshold = health_threshold
        self.canary_threshold = canary_threshold

    def __str__(self):
        s = "Policy:\n"
        s += "healthThreshold: {}\n".format(self.health_threshold)
        s += "canaryThreshold: {}\n".format(self.canary_threshold)
        return s


class MLAppNode(object):
    """
    The MLAppNode is a pipeline template that includes a name, type, pipeline template, schedule, etc.
    """
    def __init__(self, name=None,
                 id=None,
                 pipeline_pattern_id=None,
                 pipeline_agent_set=None,
                 ee_id=None):
        self.name = name
        self.id = id
        self.pipeline_pattern_id = pipeline_pattern_id
        self.ee_id = ee_id
        self.pipeline_agent_set = pipeline_agent_set

    def __str__(self):
        s = "{} Node - name: {} id: {} pipelinePatternId: {} eeId: {} agentSet {}".format(
            Constants.ION_LITERAL, self.name, self.id, self.pipeline_pattern_id,
            self.ee_id, self.pipeline_agent_set)
        return s


class Pipeline(object):
    def __init__(self, pipeline_name, pipeline_id, pipeline_type):
        self.pipeline_name = pipeline_name
        self.pipeline_id = pipeline_id
        self.pipeline_type = pipeline_type

    def __str__(self):
        s = "Pipeline - name: {} id: {}".format(self.pipeline_name, self.pipeline_id)
        return s


class EE(object):
    def __init__(self):
        self.name = None
        self.id = None
        self.agent_id = None
        self.agents = []
        self.agent_by_id = {}
        self.agent_by_hostname = {}

    def __str__(self):
        s = "EE - name: {} id: {}\n".format(self.name, self.id)
        for agent in self.agents:
            s += "\t{}\n".format(agent)
        return s


class Agent(object):
    """
    An agent is a host that runs one or more pipelines.
    """
    def __init__(self):
        self.id = None
        self.hostname = None

    def __str__(self):
        s = "Agent - hostname: {} id: {}".format(self.hostname, self.id)
        return s
