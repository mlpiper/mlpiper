"""
mlops library for reporting statistics.

This library may be used by one or more computational nodes. When run within the ParallelM context, it can query the
ParallelM service to determine which node/MLApp it belongs to and which models are currently in use (if applicable).
This library is used by processing nodes to report statistics, events, alerts, KPI, etc. to the ParallelM service.
Similarly, it can be used to query the ParallelM service to return this data to the calling program.
"""

import ast
import inspect
import logging
import pprint
from collections import OrderedDict
from datetime import datetime

import pandas as pd
import six

from parallelm.mlops.config_info import ConfigInfo
from parallelm.mlops.constants import Constants, DataframeColNames
from parallelm.mlops.data_frame import DataFrameHelper
from parallelm.mlops.events.canary_alert import CanaryAlert
from parallelm.mlops.events.data_alert import DataAlert
from parallelm.mlops.events.event import Event
from parallelm.mlops.events.event_broker import EventBroker
from parallelm.mlops.events.event_filter import EventFilter
from parallelm.mlops.events.health_alert import HealthAlert
from parallelm.mlops.events.system_alert import SystemAlert
from parallelm.mlops.ion.ion import Agent
from parallelm.mlops.logger_factory import logger_factory
from parallelm.mlops.mlops_ctx import MLOpsCtx
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.mlops_mode import MLOpsMode
from parallelm.mlops.models.model import Model
from parallelm.mlops.models.model import ModelFormat
from parallelm.mlops.models.model_filter import ModelFilter
from parallelm.mlops.models.model_helper import ModelHelper
from parallelm.mlops.singelton import Singleton
from parallelm.mlops.stats.stats_helper import StatsHelper
from parallelm.mlops.stats_category import StatCategory
from parallelm.mlops.utils import time_to_str_timestamp_milli
from parallelm.mlops.versions_info import mlops_version_info
from parallelm.mlops.mlops_rest_factory import MlOpsRestFactory
from parallelm.mlops.mlops_mode import MLOpsMode, OutputChannel

# TODO: need to make the code thread safe - so in case of multiple threads this will not crash


class MLOps(object):
    """
    Provides an API to connect ParallelM's MLOps to a user program.

    The MLOps class works as a singleton in the system.
    Note: this class should not be accessed directly but via the mlops variable which is defined in this file.

    :Example:

    >>> from parallelm.mlops import mlops
    >>> mlops.init()

    In the above example, the imported mlops variable is an instance of the MLOps class.
    """

    def __init__(self):

        self._logger = logging.getLogger(__name__)

        # Track the status of connection setup/teardown.
        self._init_called = False
        self._done_called = False

        # For use with units tests only.
        self._api_test_mode = False

        # Only if this variable is true can we access data from ParallelM services.
        self._is_pm_mode = False

        # Enable code to run in non_pm_mode
        self._connect_mlops = True
        self._input_channel_type = None
        self._output_channel_type = None
        self._output_channel = None
        self._model_helper = None
        self._stats_helper = None
        self._event_broker = None
        self._config = None
        self._mlops_ctx = None
        self._curr_model = None

    def _set_api_test_mode(self):
        self._api_test_mode = True
        self._stats_helper._api_test_mode = True

    def _set_mlops_ctx(self, mlops_ctx):
        self._mlops_ctx = mlops_ctx

    def _detect_operation_mode(self, mlops_mode):

        if mlops_mode is not None:
            # mode provided to init()
            mlops_mode_to_use = mlops_mode
        elif self._config.mlops_mode is not None:
            # mode inferred from environment variables
            mlops_mode_to_use = self._config.mlops_mode
        else:
            # default to STAND_ALONE mode
            mlops_mode_to_use = MLOpsMode.STAND_ALONE

        # This will raise an exception if mlops_mode_to_use is not recognized
        self._config.mlops_mode = MLOpsMode.from_str(mlops_mode_to_use)

    def _init_output_channel(self, ctx):
        """
        Sets the output channel according to the operation mode or detects from env
        :param ctx: Spark context (or None if not running in Spark)
        :return:
        """
        self._logger.info("setting output channel - 1 {}".format(self._config.mlops_mode))
        if self._config.mlops_mode == MLOpsMode.STAND_ALONE:
            if ctx is None:
                from parallelm.mlops.channels.file_channel import FileChannel
                self._output_channel = FileChannel()
            else:
                self._logger.info("output_channel == pyspark for Stand_Alone mode")

                from parallelm.mlops.channels.mlops_pyspark_channel import MLOpsPySparkChannel
                self._output_channel = MLOpsPySparkChannel(ctx)
                logger_factory.set_logger_provider_func(self._output_channel.get_logger)
                self._logger = logger_factory.get_logger(__name__)

        elif self._config.mlops_mode == MLOpsMode.ATTACH:
            # For now, support only python when attaching to an ION
            from parallelm.mlops.channels.mlops_python_channel import MLOpsPythonChannel
            self._output_channel = MLOpsPythonChannel(self._mlops_ctx.rest_helper(),
                                                      self._mlops_ctx.current_node().pipeline_instance_id)
        elif self._config.mlops_mode == MLOpsMode.AGENT:
            # In agent mode if the context is None, we use the python channel. Otherwise, use the pyspark channel.
            if ctx is None:
                self._logger.info("output_channel = python")
                from parallelm.mlops.channels.mlops_python_channel import MLOpsPythonChannel
                self._output_channel = MLOpsPythonChannel(self._mlops_ctx.rest_helper(),
                                                          self._mlops_ctx.current_node().pipeline_instance_id)
            else:
                self._logger.info("output_channel = pyspark")
                from parallelm.mlops.channels.mlops_pyspark_channel import MLOpsPySparkChannel
                self._output_channel = MLOpsPySparkChannel(ctx, self._mlops_ctx.rest_helper(),
                                                           self._mlops_ctx.current_node().pipeline_instance_id)
                logger_factory.set_logger_provider_func(self._output_channel.get_logger)
                self._logger = logger_factory.get_logger(__name__)
        else:
            raise MLOpsException("Mlops mode [{}] is not supported".format(self._config.mlops_mode))
        self._logger.info("setting output channel - 2 {} {}".format(self._config.mlops_mode, self._output_channel))

    def _check_init_called(self):
        if not self._init_called:
            raise MLOpsException("Must call init() before using mlops library")

    def _check_done_not_called(self):
        if self._done_called:
            raise MLOpsException("Must not call any mlops method after calling mlops.done()")

    def _verify_mlops_is_ready(self):
        self._check_init_called()
        self._check_done_not_called()

    def init(self, ctx=None, connect_mlops=False, version=None, mlops_mode=None):
        """
        This method must be called before calling any other method of this class.
        It performs initialization of the mlops module inside the current program (Spark driver, python program, etc.).

        To determine its run mode, it queries environment variables set by ParallelM agents.
        When running within ParallelM context, statistics, KPI, events, and other data reported by the calling program
        are reported to the ParallelM service. Similarly, a REST interface is constructed to query/receive data from the
        ParallelM service. For example, get_mlapp_policy() queries the ParallelM service to determine policies associated
        with the given MLApp.

        If no relevant environment variables are found or if mlops_mode is manually set to STAND_ALONE mode, output will
        be written to stdout, and methods that query the ParallelM service are not supported.

        :param ctx: Spark context to use (if running within Spark)
        :param connect_mlops: If true, work only if connected to MLOps.
        :param version: The version the code is expecting to have. If the mlops module is no longer supporting this
                version, an exception will be raised. If the value is None (default), no version check will be done.
        :param mlops_mode: which :class:`MLOpsMode` to use. If not specified, determined by environment.
        :raises: MLOpsException
        """

        str = "\n\n\n\n\ninit called: connect_mlops={}, mlops_mode={}".format(connect_mlops, mlops_mode)
        self._logger.debug(str)

        if self._init_called:
            self._logger.warning("init() already called - skipping")
            return

        # Note: in case version is None, just verify that the current version is registered.
        mlops_version_info.verify_version_is_supported(version)

        self._connect_mlops = connect_mlops
        self._config = ConfigInfo().read_from_env()
        self._logger.debug("Config:\n{}".format(self._config))
        self._detect_operation_mode(mlops_mode)

        if self._config.mlops_mode == MLOpsMode.STAND_ALONE and connect_mlops is True:
            raise MLOpsException("Detected standalone mode with connect_mlops option == True")

        if self._mlops_ctx is None:
            self._mlops_ctx = MLOpsCtx(config=self._config, mode=self._config.mlops_mode)

        self._init_output_channel(ctx)
        self._event_broker = EventBroker(self._mlops_ctx, self._output_channel)
        self._stats_helper = StatsHelper(self._output_channel)
        self._model_helper = ModelHelper(self._mlops_ctx.rest_helper(), self._mlops_ctx.ion(), self._stats_helper)
        if self._config.model_id and self._config.mlops_mode is not MLOpsMode.STAND_ALONE:
            self._curr_model = self._model_helper.get_model_obj(self._config.model_id)
            self._stats_helper._curr_model_stat = self._model_helper.get_model_stat(self._config.model_id)

        self._init_called = True
        self._done_called = False

    def done(self):
        """
        This method releases the resources obtained by the MLOps library when it was initialized.
        When called from Spark, call this method before the Spark driver completes.
        """

        self._check_init_called()
        self._logger.info("{} done() called".format(Constants.OFFICIAL_NAME))
        if self._output_channel:
            self._output_channel.done()
        self._config = None
        if self._mlops_ctx is not None:
            self._mlops_ctx.done()
        self._mlops_ctx = None
        self._done_called = True
        self._init_called = False

    @property
    def done_called(self):
        return self._done_called

    @property
    def init_called(self):
        return self._init_called

    @property
    def mlapp_id(self):
        self._verify_mlops_is_ready()
        return self._mlops_ctx.ion_id()

    def get_mlapp_id(self):
        """
        Get the current MLApp id.
        For example: "0de31941-76f9-40af-a71f-5d5e10298176"

        :return: MLApp id
        :rtype: string
        :raises: MLOpsException

        """
        self._verify_mlops_is_ready()
        return self.mlapp_id

    @property
    def mlapp_name(self):
        self._verify_mlops_is_ready()
        return self._mlops_ctx.ion_name()

    def _pipeline_inst_id(self):
        self._verify_mlops_is_ready()
        mlapp_node = self.get_current_node()
        return mlapp_node.pipeline_instance_id

    def get_mlapp_name(self):
        """
        Get the current MLApp name

        :return: MLApp name
        :rtype: string
        :raises: MLOpsException
        """
        self._verify_mlops_is_ready()
        return self.mlapp_name

    @property
    def mlapp_policy(self):
        self._verify_mlops_is_ready()
        return self._mlops_ctx.ion_policy()

    def get_mlapp_policy(self):
        """
        Get the current MLApp policy
        For example: "ALWAYS_UPDATE" is a policy to always propagate new models between pipeline producers and
        pipeline consumers.

        :return: MLApp policy.
        :rtype: string
        :raises: MLOpsException

        """
        self._verify_mlops_is_ready()
        return self.mlapp_policy

    @property
    def nodes(self):
        self._verify_mlops_is_ready()
        return self._mlops_ctx.ion_nodes()

    def get_nodes(self):
        """
        Return a list of all the Node objects that are part of the current MLApp

        :return: list of MLAppNodes
        :raises: MLOpsException

        """
        self._verify_mlops_is_ready()
        return self.nodes

    def get_node(self, name):
        """
        Return the MLAppNode object that corresponds to the given name.

        :param name: name of component to look for
        :return: MLAppNode object if found; otherwise, None
        :raises: MLOpsException

        """
        self._verify_mlops_is_ready()
        return self._mlops_ctx.get_ion_node(name)

    def get_current_node(self):
        """
        Return the current node

        :return: the current node
        :raises: MLOpsException
        """
        self._verify_mlops_is_ready()
        return self._mlops_ctx.current_node()

    def get_agents(self, node_name):
        """
        Return a list of Agent objects corresponding to the agents used by this component.

        :param node_name: return only agents running this node
        :return: list of agents
        :raises: MLOpsException

        """
        self._verify_mlops_is_ready()
        return self._mlops_ctx.get_ion_node_agents(node_name)

    def get_agent(self, node_name, agent_id):
        """
        Return the agent with the corresponding agent id.

        :param node_name: return an agent running this node
        :param agent_id: the id of the agent
        :return: the agent
        :raises: MLOpsException
        """
        self._verify_mlops_is_ready()
        agent_list = self._mlops_ctx.get_ion_node_agents(node_name)
        for agent in agent_list:
            if agent.id == agent_id:
                return agent
        return None

    def current_model(self):
        """
        Return a Model object with information about the current model used. This is only valid if the node is a
        model consumer (e.g., inference) node.

        :return: Model object if relevant; otherwise, None
        """
        return self._curr_model

    def set_stat(self, name, data=None, category=StatCategory.TIME_SERIES, timestamp=None):
        """
        Report this statistic.

        :param name: name to use in the export
        :param data: data object to export
        :param category: category of the statistic. One of :class:`StatsCategory`
        :param timestamp: optional timestamp
        :raises: MLOpsException
        """
        self._verify_mlops_is_ready()
        self._stats_helper.set_stat(name, data, None, category, timestamp)

    def set_kpi(self, name, data, timestamp=None, units=None):
        """
        Exports KPI data to the PM service. Users may supply a timestamp, which allows older data to be loaded.

        :param name: KPI name, which will be displayed in the UI. It can be used to fetch the stored data
                     at a later time.
        :param data: The data to store. Currently the only expected data type is a line graph, which consists of
                     discrete numeric values
        :param timestamp: The timestamp is a given units (Optional). If not provided, the current time is assumed
        :param units: The timestamp units. One of: KpiValue.TIME_SEC, KpiValue.TIME_MSEC, KpiValue.TIME_NSEC
        :return: The current PM instance for further calls
        :raises: MLOpsException
        """
        self._verify_mlops_is_ready()
        self._stats_helper.set_kpi(name, data, None, timestamp, units)
        return self

    def set_data_distribution_stat(self, data, model=None, timestamp=None):
        """
        Exports distribution statistics which will be shown in Health View.

        :param data: The data that represents distribution. Data must have specific type according to engine.
                     For PyStark engine: RDD or DataFrame.
                     For Python engine: Numpy ND array or Pandas DataFrame
                     Currently the only expected data type is a line graph, which consists of
                     discrete numeric values
        :param model: For PySpark engine: model is used to classify categorical and continuous features.
        :param timestamp: The timestamp is a given units (Optional). If not provided, the current time is assumed
        :raises: MLOpsException
        """
        self._verify_mlops_is_ready()
        self._stats_helper.set_data_distribution_stat(data, None, model, timestamp)

    def get_data_distribution_stats(self, model_id):
        """
        Return a dataframe with statistics for the given model ID.


        :param model_id:
        :return:
        """

        self._verify_mlops_is_ready()
        if model_id is None:
            raise MLOpsException("model_id argument can not be None")

        if not isinstance(model_id, six.string_types):
            raise MLOpsException("model_id argument must be of a string type given type: {}".format(type(model_id)))

        if self._api_test_mode:
            self._logger.info("API testing mode - returning without performing call")
            return

        json = self._mlops_ctx.rest_helper().get_model_stats(model_id=model_id)
        self._logger.debug("JSON = {}".format(json))

        dfh = DataFrameHelper()
        stat_df_list = []
        for model_stat in json:
            # TODO: check type and make sure it is ok
            data = model_stat['data']
            # Convert data to a dict
            decoded_data = ast.literal_eval(data)

            bargraph_data = decoded_data['data']
            bargraph_dict = ast.literal_eval(bargraph_data)

            hist_df = dfh.multi_histogram_df(bargraph_dict)
            # TODO: need to add the modle ID and timestamp columns
            dfh.add_col_with_same_value(hist_df, "modelId", model_id)
            dfh.add_col_with_same_value(hist_df, "timestamp", decoded_data['timestamp'])
            stat_df_list.append(hist_df)

        final_df = pd.concat(stat_df_list, ignore_index=True)
        return final_df

    def _verify_get_stats_args(self, name, node, agent):
        if name is None:
            msg = "Statistic name can not be None"
            self._logger.error(name)
            raise MLOpsException(msg)

        if node is None and agent is not None:
            raise MLOpsException("{} node was given as None, thus agent must be None too".format(
                Constants.ION_LITERAL))

        if agent is None:
            pass
        elif isinstance(agent, six.string_types):
            pass
        elif isinstance(agent, Agent):
            pass
        else:
            raise MLOpsException("Agent parameter is not a string or Agent object")

    def _verify_time_window(self, start_time, end_time, allow_none=False):
        if allow_none is False:
            if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
                raise MLOpsException("time argument should be of type datetime.datetime")

            if start_time > end_time:
                raise MLOpsException("start time is bigger than end time")
        else:
            if start_time is not None:
                if not isinstance(start_time, datetime):
                    raise MLOpsException("time argument should be of type datetime.datetime")
            if end_time is not None:
                if not isinstance(start_time, datetime):
                    raise MLOpsException("time argument should be of type datetime.datetime")
            if start_time is not None and end_time is not None:
                if start_time > end_time:
                    raise MLOpsException("start time is bigger than end time")

    def _assemble_nodes_and_agents(self, ion, node, agent):

        # Assemble a list of nodes and for each node the list of agents.
        agents_per_node = OrderedDict()
        if node is not None:
            node_obj = self.get_node(node)
            if node_obj is None:
                msg = "{} Node: [{}] is not present in {}".format(Constants.ION_LITERAL, node,
                                                                  Constants.ION_LITERAL)
                self._logger.error(msg)
                raise MLOpsException(msg)
            if agent is None:
                agents_per_node[node_obj.name] = self.get_agents(node_obj.name)
            else:
                if isinstance(agent, six.string_types):
                    agent_id = agent
                else:
                    agent_id = agent.id
                agent_obj = self.get_agent(node_obj.name, agent_id)
                if agent_obj is None:
                    raise MLOpsException("Agent: {} is not found in node {}".format(agent, node_obj.name))
                agents_per_node[node_obj.name] = [agent_obj]
        else:
            node_list = ion.node_by_id.values()
            for node_obj in node_list:
                agents_per_node[node_obj.name] = self.get_agents(node_obj.name)

        return agents_per_node

    def get_stats(self, name, mlapp_node, agent, start_time, end_time, no_of_lines=-1):
        """
        Get statistics from MLOps.
        This call provides a way for python code uploaded to MLOps to obtain access to
        MLApp statistics. The get_stats() call allows access to statistics from each of the MLApp components; these may be
        filtered by the agent. The start_time and end_time can be used to define a time window for the returned stats.

        :param name: The name of the statistic - this is the name used in the stats method
        :param mlapp_node: The name of the MLApp node
        :param agent: ID of the agent which collected the statistics
        :param start_time: A datetime object representing the start of the time window
        :param end_time: A datetime object representing the end of the time window
        :param no_of_lines: DEPRECATED
        :return: A dataframe representing the statistics collected over time
        :raises: MLOPsException

        :Example:

        >>> from parallelm.mlops import mlops as pm
        >>> from datetime import datetime, timedelta
        >>> node_agent_list = pm.get_agents("0")
        >>> now = datetime.utcnow()
        >>> hour_ago = (now - timedelta(hours=1))
        >>> df = pm.get_stats("myValue", "0",  node_agent_list[0].id, hour_ago, now)

        The above example assumes the MLApp this code is running as part of has a component called "my_training".
        1. The code obtains the list of agents which are used by the "my_training" component.
        2. It computes the timestamp of now.
        3. It computes the timestamp of an hour ago.
        4. It calls the get_stats method asking for the "myValue" statistic from the first agent.
        The return value is a dataframe object.


        """
        self._verify_mlops_is_ready()

        ion = self._mlops_ctx.ion()

        self._verify_get_stats_args(name, mlapp_node, agent)
        self._verify_time_window(start_time, end_time)

        start_time_ms_timestamp = time_to_str_timestamp_milli(start_time)
        end_time_ms_timestamp = time_to_str_timestamp_milli(end_time)
        self._logger.info("Calling get_stat with start_time: {} end_time: {}".format(
            start_time_ms_timestamp, end_time_ms_timestamp))

        if self._api_test_mode:
            self._logger.info("API testing mode - returning without performing call")
            return

        # A valid system should exist past this point

        agents_per_node = self._assemble_nodes_and_agents(ion, mlapp_node, agent)
        self._logger.info("agents_per_node:" + str(agents_per_node))
        df_helper = DataFrameHelper()

        df_list = []
        for node_name in agents_per_node.keys():
            node_obj = self.get_node(node_name)
            self._logger.info("{} component: {}".format(Constants.ION_LITERAL, node_name))

            for agent_obj in agents_per_node[node_name]:
                self._logger.info("stat: {} {} node: {} agent: {}".format(name,
                                                                          Constants.ION_LITERAL,
                                                                          node_obj.name,
                                                                          agent_obj.id))

                json = self._mlops_ctx.rest_helper().get_stat(stat_name=name,
                                                              ion_id=ion.id,
                                                              workflow_node_id=node_obj.id,
                                                              agent_id=agent_obj.id,
                                                              pipeline_id=
                                                              node_obj.pipeline_agent_set[
                                                                  agent_obj.id],
                                                              start_time=start_time_ms_timestamp,
                                                              end_time=end_time_ms_timestamp)

                # TODO: next set of patches will convert the JSON into a dataframe with the correct format
                self._logger.info(pprint.pformat(json))
                df = df_helper.create_data_frame(json, no_of_lines)
                df_helper.add_col_with_same_value(df, DataframeColNames.ION_NODE, node_name)
                df_helper.add_col_with_same_value(df, DataframeColNames.AGENT, agent_obj.id)
                df_list.append(df)
        final_df = pd.concat(df_list, ignore_index=True)
        return final_df

    def get_kpi(self, name, start_time, end_time):
        """
        Return KPI statistics withina given time window

        :param name: statistic name
        :param start_time: start of time window
        :param end_time: end of time window
        :return: Dataframe with the KPI statistics values
        """
        return self.get_stats(name, mlapp_node=None, agent=None, start_time=start_time, end_time=end_time)

    def get_models_by_time(self, start_time, end_time, download=False, pipeline_name=None):
        """
        Retrieve models in the context of the current MLApp from MLOps based on start and end times.

        :param start_time: a datetime object specifying window start time
        :type start_time: datetime
        :param end_time: a datetime object specifying window end time
        :type end_time: datetime
        :param download: If true, download the model data and provide it as an additional column in the dataframe
        :type download: bool
        :param pipeline_name: query by pipeline
        :type pipeline_name: string
        :return: Spark or pandas dataframe based on mode with the models as a byte array
        """

        self._verify_mlops_is_ready()

        self._verify_time_window(start_time, end_time)

        ion = self._mlops_ctx.ion()

        if self._api_test_mode:
            self._logger.info("API testing mode - returning without performing call - in {}".format(
                inspect.stack()[0][3]))

        model_filter = ModelFilter()
        model_filter.time_window_start = start_time
        model_filter.time_window_end = end_time
        if pipeline_name is not None:
            if pipeline_name not in ion.pipeline_by_name:
                raise MLOpsException("Error: invalid pipeline name {}".format(pipeline_name))
            pipeline_instances = [x[1] for x in ion.pipeline_to_pipelineInstances[pipeline_name]]
            model_filter.pipeline_instance_id = pipeline_instances

        model_df = self._model_helper.get_models_dataframe(model_filter=model_filter, download=download)
        return model_df

    def get_model_by_id(self, model, download=False):
        """
        Return the model with this id

        :param model: the model id
        :param download: Boolean, whether to download the model
        :return: return model as a bytearray
        :raises: MLOpsException
        """
        self._verify_mlops_is_ready()

        if isinstance(model, six.string_types):
            model_id = model
        elif isinstance(model, Model):
            model_id = model.id
        else:
            raise MLOpsException("model parameter can be either a string or of class Model: got [{}]".format(
                type(model)))

        if self._api_test_mode:
            self._logger.info("API testing mode - returning without performing call - in {}".format(
                inspect.stack()[0][3]))

        model_filter = ModelFilter()
        model_filter.id = model_id
        model_df = self._model_helper.get_models_dataframe(model_filter=model_filter, download=download)

        return model_df

    def get_last_approved_model(self):
        mlapp_id = self.mlapp_id
        pipeline_inst_id = self._pipeline_inst_id()

        return self._model_helper.get_last_approved_model(mlapp_id, pipeline_inst_id)

    def data_alert(self, title, desc, data=None):
        """
        Inject a data alert into the PM system.
        Data alerts refer to discrete anomalies in input data.
        The alert will be displayed in the timeline pane in the dashboard.
        The data alert counter will be incremented.

        :param title: A short description used as the title
        :param desc: A long description, which will be displayed in the timeline pane in the UI
        :param data: Any python object, which will be serialized (pickle) and stored in the PM system. It can later
                     be fetched for further manipulation.
        :return: The current mlops instance for further calls
        :raises: MLOpsException
        """
        self._verify_mlops_is_ready()

        self.event(DataAlert(title, desc, data))
        return self

    def health_alert(self, title, desc, data=None):
        """
        Inject a health alert into the PM system.
        Health alerts may refer to issues concerning the algorithm's functioning, which may lead to wrong results,
        for instance, due to deviation in data distribution.
        The alert will be displayed in the timeline pane in the PM dashboard.
        The health alert counter will be incremented.

        :param title: A short description that is used as the title
        :param desc: A long description, which will be displayed in the timeline pane in the UI
        :param data: Any python object, which will be serialized (pickle) and stored in PM system. It can later
                     be fetched for further manipulation.
        :return: The current mlops instance for further calls
        :raises: MLOpsException

        """
        self._verify_mlops_is_ready()
        self.event(HealthAlert(title, desc, data))
        return self

    def system_alert(self, title, desc, data=None):
        """
        Inject a system alert into the PM system.
        System alerts may refer to any environmental issues, such as invalid permissions, wrong file paths, wrong URLs,
        and so forth.
        The alert will be displayed in the timeline pane in the PM dashboard.
        The system alert counter will be incremented.

        :param title: A short description that is used as the title
        :param desc: A long description, which will be displayed in the timeline pane in the UI
        :param data: Any python object, which will be serialized (pickle) and stored in PM system. It can later
                     be fetched for further manipulation.
        :return: The current mlops instance for further calls
        :raises: MLOpsException

        """
        self._check_done_not_called()
        self._check_done_not_called()
        self.event(SystemAlert(title, desc, data))
        return self

    def canary_alert(self, title, is_healthy, score, threshold):
        """
        Inject a canary alert to the PM system. The alert will be displayed in the timeline pane of Health View and
        Data Scientist Views. In addition, the canary alerts counter will be incremented all dashboards. Canary alerts
        return the results of comparing the predictions from 2 pipelines.

        :param title: A short description that is used as the title
        :param is_healthy: whether the canary pipeline is healthy
        :param score: the canary comparison score
        :param theshold: the canary comparison threshold
        :return: The current PM instance for further calls
        :raises: MLOpsException

        """
        self._verify_mlops_is_ready()
        self.event(CanaryAlert(title, is_healthy, score, threshold))
        return self

    def event(self, event_obj):
        """
        Generate an event which is sent to MLOps.

        :param event_obj: Object of type :class:`Event` (can be inheriting from Event)
        :return: The current mlops instance for further calls
        :raises: MLOpsException
        """
        self._verify_mlops_is_ready()

        if not isinstance(event_obj, Event):
            raise MLOpsException("Event object must be an instance of Event class")

        self._event_broker.send_event(event_obj)
        return self

    def set_event(self, name, type=None, data=None, is_alert=False, timestamp=None):
        """
        Generate an event which is sent to MLOps.

        :param name: name for the event
        :param type: type of :class:`Event`
        :param data: Any python object, which will be serialized (pickle) and stored in the PM System. It can later
                     be fetched for further manipulation.
        :param is_alert: Boolean indicating whether this event is an alert.
        :param timestamp: optional datetime.datetime timestamp. If None, the current timestamp will be provided.
        :return: The current mlops instance for further calls
        :raises: MLOpsException
        """
        self._verify_mlops_is_ready()

        if name is None:
            raise MLOpsException("Name argument must be a string or an event object")

        if isinstance(name, six.string_types):
            if type is None:
                raise MLOpsException("Type of event can not be None")
            event_obj = Event(label=name, event_type=type, description=None, data=data,
                              is_alert=is_alert, timestamp=timestamp)

        elif isinstance(name, Event):
            event_obj = name
        else:
            raise MLOpsException("Name argument can be a string or event object only")

        if self._api_test_mode:
            self._logger.info("API testing mode - returning without performing call")
            return

        self._event_broker.send_event(event_obj)
        return self

    def get_events(self, type=None, mlapp_node=None, agent=None, start_time=None, end_time=None, is_alert=True):
        """
        Fetch events according to a time window and other filters.

        :param mlapp_node: Set the mlapp_node to select events from. If None, events will be fetched for the entire ION.
        :param agent: (optional) Either an agent ID or an agent object. If specified, the query will filter by agent.
        :param start_time: (optional) Filter by start time, this should be a datetime object.
        :param end_time: (optional) Filter by end time, this should be a datetime object.
        :param is_alert: (optional) Filter by is_alert - between alerts and events. Default is True. If None is provided
                         all events are fetched.
        :return: A dataframe containing information about the events obtained.
        :raises: MLOpsException

        :Example:

        >>> from parallelm.mlops import mlops as pm
        >>> from datetime import datetime, timedelta
        >>> node_agent_list = pm.get_agents("0")
        >>> now = datetime.utcnow()
        >>> hour_ago = (now - timedelta(hours=1))
        >>> df = pm.get_events("training_comp",  node_agent_list[0].id, hour_ago, now)

        The above example assumes the MLApp this code is running as part of a node called "my_training".
        1. The code obtains the list of agents which are used by the "my_training" component.
        2. It computes the timestamp of now.
        3. It computes the timestamp of an hour ago.
        4. It calls the get_events method asking for all the events generated by the "0" node of the first
        agent during the past hour.

        """

        self._verify_mlops_is_ready()
        self._verify_time_window(start_time, end_time, allow_none=True)

        start_time_ms_timestamp = time_to_str_timestamp_milli(start_time) if start_time is not None else None
        end_time_ms_timestamp = time_to_str_timestamp_milli(end_time) if end_time is not None else None

        ion = self._mlops_ctx.ion()

        # Translate the mlapp_node to its pipeline_instance_id
        pipeline_inst_id = None
        if mlapp_node is not None:
            mlapp_node = self.get_node(mlapp_node)
            if mlapp_node is None:
                raise MLOpsException("No such {} node: [{}]".format(Constants.ION_LITERAL,
                                                                    mlapp_node))
            pipeline_inst_id = mlapp_node.pipeline_instance_id

        # Translate the agent to its host name
        agent_host = None
        if agent is not None:
            if isinstance(agent, Agent):
                agent_host = agent.hostname
            elif isinstance(agent, six.string_types):
                agent_obj = self._mlops_ctx.get_agent_by_id(agent)
                if agent_obj is None:
                    raise MLOpsException("Agent ID [{}] does not " +
                                         "exists in this {}".format(agent, Constants.ION_LITERAL))
                agent_host = agent_obj.host

        if self._api_test_mode:
            self._logger.info("API testing mode - returning without performing call")
            return

        ef = EventFilter()
        ef.ion_name = None
        ef.ion_inst_id = ion.id
        ef.pipeline_inst_id = pipeline_inst_id
        ef.agent_host = agent_host
        ef.time_window_start = start_time_ms_timestamp
        ef.time_window_end = end_time_ms_timestamp
        ef.is_alert = is_alert

        events_df = self._event_broker.get_events(ef)
        return events_df

    def publish_model(self, model):
        """
        Exports Model to the PM service.
        Model data and metada must be set using :class:`Model`

        :param model: Object of type :class:`Model`
        :return: The model Id
        :raises: MLOpsException
        """
        self._verify_mlops_is_ready()

        if not isinstance(model, Model):
            raise MLOpsException("Model object must be an instance of Model class")

        model_id = self._model_helper.publish_model(model, self._config.pipeline_id)

        return model_id

    def Model(self, name="", model_format=ModelFormat.UNKNOWN, description="", user_defined=""):
        return self._model_helper.create_model(name=name,
                                               model_format=model_format,
                                               description=description,
                                               user_defined=user_defined)

    def load_time_capture(self, input_file):
        from parallelm.mlops.time_capture.time_capture import TimeCapture
        return TimeCapture(input_file)

    def attach(self,
               mlapp_id,
               mlops_server=Constants.MLOPS_DEFAULT_HOST,
               mlops_port=Constants.MLOPS_DEFAULT_PORT,
               user=Constants.MLOPS_DEFAULT_USER,
               password=None):
        """
        Attach to a running MLApp and run in its context.
        Side effect: sets up mlops_context
        :param mlapp_id: the id of the MLApp to connect to
        :param mlops_server: the host to connect to
        :param mlops_port: the port MLOps is using
        :param user: user name to use for connection
        :param password: password to use for authentication
        :return:
    `
        Note: Attach only works for pure python code
        """
        self._logger.info("Connecting to mlops: {} {}: {} user: {} pass: {}".format(
            mlops_server, Constants.ION_LITERAL, mlapp_id, user, password))

        # Connecting directly the server
        rest_helper = MlOpsRestFactory().get_rest_helper(MLOpsMode.ATTACH, mlops_server, mlops_port, None)
        token = rest_helper.login(user, password)

        # Setting the environment for mlops
        ci = ConfigInfo()
        ci.token = token
        ci.zk_host = None
        ci.mlops_port = str(mlops_port)  # Constants.MLOPS_DEFAULT_PORT
        ci.mlops_server = mlops_server
        ci.ion_id = mlapp_id
        ci.mlops_mode = MLOpsMode.ATTACH
        ci.output_channel_type = OutputChannel.PYTHON

        # TODO: for now assume node "0" - allow providing the node id or just become any node
        ci.ion_node_id = "0"
        ci.pipeline_id = "0"

        self._logger.info("MLOps configuration:\n{}".format(ci))
        ci.set_env()

        # calling init
        self.init(ctx=None, mlops_mode=MLOpsMode.ATTACH)



# An instance of the MLOps to be used when importing the pm library.
@Singleton
class MLOpsSingleton(MLOps):
    pass


mlops = MLOpsSingleton.Instance()
