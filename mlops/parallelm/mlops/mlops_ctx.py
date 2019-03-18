from kazoo.client import KazooClient
import copy
import time
import six

from parallelm.mlops.mlops_mode import MLOpsMode
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.constants import Constants
from parallelm.mlops.ion.ion_builder import IONBuilder, IONJsonConstants
from parallelm.mlops.ion.ion import ION
from parallelm.mlops.ion.ion import EE, Agent, Policy
from parallelm.mlops.base_obj import BaseObj
from parallelm.mlops.mlops_rest_factory import MlOpsRestFactory
from parallelm.mlops.mlops_rest_interfaces import MlOpsRestHelper
from parallelm.mlops.config_info import ConfigInfo


class MLOpsCtx(BaseObj):
    """
    Provide context information for MLOps library.
    This is an internal class which should not be exposed to users of MLOps.
    The object contains the structure of the ION and groups and such.
    """

    def __init__(self, config, mode=None):
        """
        Perform initialization of the MLOpsCtx.
        It expects configuration from the environment to arrive in the standard usage mode.
        :param config: :class:`ConfigInfo` for this MLOps instantiation
        :param mode: python or pyspark
        :return:
        : raises MLOpsException for invalid configurations
        """
        super(MLOpsCtx, self).__init__(__name__)

        self._info("MLOpsCtx __init__ called")
        self._info("Config\n{}".format(config))
        self._agent_list = []
        self._rest_helper = None

        self._ci = config
        self._mode = mode

        self._ion = None  # Will contain an ION class corresponding to the active ION which this mlops is part of
        self._ees_dict = {}  # Will contain all ees defined with the agent inside
        self._agents_dict = {}
        self._rest_helper = MlOpsRestFactory().get_rest_helper(self._mode, self._ci.mlops_server, self._ci.mlops_port, self._ci.token)

        if self._mode == MLOpsMode.AGENT:
            # In agent mode, we talk with the agent and use the mlops prefix to the http requests
            self._validate_config()
            self._info("Agent mode")

            self._rest_helper.set_prefix(Constants.URL_MLOPS_PREFIX)

            json_dict = self._detect_ion_structure()
            health_json_dict = self._fetch_health_thresholds()
            self._detect_ees_and_agents()
            self._build_ion_obj(json_dict)
            self._build_health_obj(health_json_dict)

        elif self._mode == MLOpsMode.ATTACH:
            # In attach mode, we connect either to the ZK or to the server directly

            if self._ci.zk_host:
                self._detect_mlops_server_via_zk()
            self._validate_config()
            self._info("In pm mode - will try to connect to server")

            ion_json_dict = self._detect_ion_structure()
            health_json_dict = self._fetch_health_thresholds()
            self._detect_ees_and_agents()
            self._build_ion_obj(ion_json_dict)
            self._build_health_obj(health_json_dict)

        elif self._mode == MLOpsMode.STAND_ALONE:
            # In stand alone mode, we do not have a valid ION structure

            self._logger.info("In stand-alone mode: ctx data will not be available")
            self._set_stand_alone_values()
        else:
            raise MLOpsException("Unsupported operation mode: {}".format(self._mode))

    def _fetch_health_thresholds(self):
        return self._rest_helper.get_health_thresholds(self._ci.ion_id)

    def _set_stand_alone_values(self):
        self._ion = ION()
        self._ion.id = 1
        self._ion.name = "ION_1"

    def _validate_config(self):
        """
        Validate that all config information is present
        :return:
        :raises MLOpsException for invalid configurations
        """
        if self._ci.token is None:
            raise MLOpsException("Internal Error: No auth token provided")

        if self._ci.mlops_server is None or self._ci.mlops_port is None:
            raise MLOpsException("MLOps server host or port were not provided")

        if self._ci.ion_id is None:
            MLOpsException("{} instance id not provided".format(Constants.ION_LITERAL))

    def _detect_mlops_server_via_zk(self):
        """
        Detect the active mlops server via the ZK
        :return:
        """
        zk = None
        try:
            zk = KazooClient(hosts=self._ci.zk_host, read_only=True)
            zk.start()
            if zk.exists(Constants.MLOPS_ZK_ACTIVE_HOST_PORT):
                data, stat = zk.get(Constants.MLOPS_ZK_ACTIVE_HOST_PORT)
                eco_host_port = data.decode("utf-8").split(':')

                if len(eco_host_port) is 2:
                    self._ci.mlops_server = eco_host_port[0]
                    self._ci.mlops_port = eco_host_port[1]
                else:
                    raise MLOpsException("Internal Error: Invalid zookeeper active server entry, host_port: {}"
                                         .format(eco_host_port))
            else:
                raise MLOpsException("Unable to connect to the active MLOps server, zk_host: {}"
                                     .format(self._ci.zk_host))
        except Exception as e:
            raise MLOpsException("{}, zk_host: {}".format(e, self._ci.zk_host))
        finally:
            if zk:
                zk.stop()

    @staticmethod
    def _search_list_dict(kv, key, value):
        for x in kv:
            if x[key] == value:
                return x
        return None

    def _detect_ion_structure(self):
        """
        Detect the current ion structure (pipeline, groups, agents, etc.)
        :return:
        :raises MLOpsException if ION or other structures are not found
        """
        self._info("Detecting {} structure".format(Constants.ION_LITERAL))

        # This is the max number of retries to wait until the ION is running.
        # The pipelineInstance part of the workflow description does not appear until the ION
        # switches to RUNNING state. For this reason, the code loop until the pipelineInstances part
        # appears in the JSON.
        max_tries = Constants.WAIT_FOR_PIPELINE_INSTANCE_TO_APPEAR_TIMEOUT

        wf_instance = {}
        found = False
        for idx in range(0, max_tries):

            wf_instance = self._rest_helper.get_workflow_instance(self._ci.ion_id)
            if wf_instance is None:
                raise MLOpsException("Could not locate {} instance {}".format(
                    Constants.ION_LITERAL, self._ci.ion_id))

            self._debug("{} status: {}".format(Constants.ION_LITERAL, wf_instance['status']))

            if IONJsonConstants.PIPELINE_INSTANCES_SECTION in wf_instance:
                found = True
                break
            self._info("Could not find {} in workflow json - try {}".format(
                IONJsonConstants.PIPELINE_INSTANCES_SECTION, idx))
            time.sleep(1)

        if found is False:
            raise MLOpsException("Could not find {} section in workflow information".format(
                IONJsonConstants.PIPELINE_INSTANCES_SECTION))

        ion_json_dict = wf_instance
        self._debug("workflow: {}".format(ion_json_dict))
        return ion_json_dict

    def _detect_ees_and_agents(self):
        ees_json_dict = self._rest_helper.get_ees()
        agents_json_dict = self._rest_helper.get_agents()

        self._debug("Agents JSON:\n{}\n\n".format(agents_json_dict))
        self._debug("EEs JSON:\n{}\n\n".format(ees_json_dict))

        # Generating a dict of all agents by ID

        for agent_json in agents_json_dict:
            agent_obj = Agent()
            agent_obj.id = str(agent_json["id"])
            agent_obj.hostname = str(agent_json["address"])
            self._agents_dict[agent_obj.id] = agent_obj

        for ee_json in ees_json_dict:
            ee = EE()
            ee.name = str(ee_json["name"])
            ee.id = str(ee_json["id"])
            ee.agent_id = str(ee_json["agentId"])

            # get agent object we created above in the agent_dict
            if ee.agent_id not in self._agents_dict:
                raise MLOpsException("EE {} contains Agent {} which is not in global agent list".format(
                    ee.name, ee.agent_id))

            agent_obj = self._agents_dict[ee.agent_id]
            ee.agents.append(agent_obj)
            ee.agent_by_id[ee.agent_id] = agent_obj
            ee.agent_by_hostname[agent_obj.hostname] = agent_obj

            self._ees_dict[ee.id] = ee
            self._logger.info("EE:\n{}".format(ee))

    def _build_ion_obj(self, ion_json_dict):
        ion_builder = IONBuilder()
        self._ion = ion_builder.build_from_dict(ion_json_dict)
        # This info line is important so we can understand what was the ion structure if errors happens at customer site
        self._info("{}:\n{}".format(Constants.ION_LITERAL, self._ion))
        self._info("---------------------")

    def _build_health_obj(self, health_json_dict):
        self._ion.policy.set_thresholds(health_json_dict[IONJsonConstants.ION_GLOBAL_THRESHOLD_TAG], health_json_dict[IONJsonConstants.ION_CANARY_THRESHOLD_TAG])

    def rest_helper(self):
        return self._rest_helper

    def ion(self):
        """
        Return a copy of the ion object
        :return: ION object
        :rtype: ION
        """
        return copy.deepcopy(self._ion)

    def ion_id(self):
        """
        Return the ION id
        :return:
        """
        return self._ion.id

    def ion_node_id(self):
        """
        Return the current node id this code is running in
        :return: ION node id
        """
        return self._ci.ion_node_id

    def current_node(self):
        if self._ci.ion_node_id not in self._ion.node_by_id:
            raise MLOpsException("Current node id: [{}] is not detected in {}".format(
                self._ci.ion_node_id, Constants.ION_LITERAL))
        return self._ion.node_by_id[self._ci.ion_node_id]

    def ion_name(self):
        """
        Return the ION name
        :return:
        """
        return self._ion.name

    def ion_policy(self):
        """
        Return the ION policy
        :return:
        """
        return self._ion.policy

    def ion_nodes(self):
        """
        Return a list of ION components
        :return:
        """
        return copy.deepcopy(self._ion.nodes)

    def get_ion_node(self, name):
        """
        Return a component object given its name
        :param name:
        :return: Component object matching name (if found), None if not found
        """
        self._debug("Getting {} component [{}]".format(Constants.ION_LITERAL, name))
        self._debug("{} comps by name: {}".format(Constants.ION_LITERAL, self._ion.node_by_name))
        if name in self._ion.node_by_name:
            return self._ion.node_by_name[name]
        return None

    def get_ion_node_by_pipeline_instance(self, pipe_instance_id):
        """
        Return a list of ion_nodes for the given pipeline instance.
        :param pipe_instance_id: id of the pipeline
        :return: ion_nodes or None if not found
        """
        if pipe_instance_id in self._ion.node_by_pipe_inst_id:
            return self._ion.node_by_pipe_inst_id[pipe_instance_id]
        return None

    def get_ion_node_agents(self, node):
        """
        Return a list of agents for the given ion_node
        :param node: a node within the ION
        :return: List of agents for the given ion_node
        :raises MLOpsException for invalid arguments
        """

        # Getting by component name
        if isinstance(node, six.string_types):
            if node not in self._ion.node_by_name:
                raise MLOpsException("Node {} is not part of current {}".format(
                    node, Constants.ION_LITERAL))

            node_obj = self._ion.node_by_name[node]
            if node_obj.ee_id not in self._ees_dict:
                raise MLOpsException("Component {} had ee_id {} which is not part of valid ees".format(
                    node_obj.name, node_obj.ee_id))

            ee_obj = self._ees_dict[node_obj.ee_id]
            # Note: calling deepcopy in order for the user to get a copy of agents objects and not point to internal
            # data
            agent_list = copy.deepcopy(ee_obj.agents)
            return agent_list
        else:
            raise MLOpsException("component argument should be component name (string)")

    def get_agent_by_id(self, agent_id):
        """
        Return agent object by ID, assuming the agent is part of the current ION
        :param agent_id: Agent Id to search for
        :type agent_id: str
        :return: agent_id or None if not found
        """

        if agent_id in self._agents_dict:
            return self._agents_dict[agent_id]
        return None

    def done(self):
        if self._rest_helper is not None:
            self._rest_helper.done()
