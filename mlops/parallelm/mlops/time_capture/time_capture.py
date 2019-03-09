import os
import shutil
import tempfile
import datetime
from parallelm.mlops.time_capture.parsers import Parsers
from parallelm.mlops.time_capture.time_functions import TimeFunctions
from parallelm.mlops.mlops_exception import MLOpsException


class TimeCapture (Parsers):
    """This class handles the MCenter time capture tar file and extract from it plots
    of different stats
    """
    def __init__(self, input_timeline_capture):
        """Initialized the parameters of the time capture stats."""
        self._heatmap_df_file = {}
        self._matrix_df_file = {}
        self._multilinegraph_df_file = {}
        self._multigraph_df_file = {}
        self._bar_df_file = {}
        self._kpi_df_file = {}
        self._sys_stat_df_file = {}
        self._aggregate_df_file = {}
        self._events_df_file = {}
        self._graph_type = ""
        self._file_names = []
        self._attribute_names_list = []
        self._df_name = {}
        self._files = {}
        self._mlapp_id = ""
        self._model_policy = ""
        self._nodes_number = 0
        self._nodes_id = []
        self._nodes_type = []
        self._time_functions = TimeFunctions()
        Parsers.__init__(self, input_timeline_capture, tempfile.mkdtemp())

    def save_to_output(self, output_path):
        """
         The method saves files to the output path.

         :param self:
         :param output_path: Output path
         :return:
         """
        try:
            try:
                shutil.rmtree(output_path)
            except Exception as err:
                pass
            os.makedirs(output_path)
            for file_name in self._file_names:
                try:
                    os.remove(output_path + "/" + str(file_name))
                except Exception as err:
                    pass  # nothing to erase
                with open(output_path + "/" + str(file_name), 'w') as f:
                    f.write(self._files[file_name])

            self._save_df_to_output(output_path, self._bar_df_file, "bargraph")
            self._save_df_to_output(output_path, self._multilinegraph_df_file, "multilinegraph")
            self._save_df_to_output(output_path, self._heatmap_df_file, "heatmap")
            self._save_df_to_output(output_path, self._multigraph_df_file, "multigraph")
            self._save_df_to_output(output_path, self._matrix_df_file, "matrix")
            self._save_df_to_output(output_path, self._kpi_df_file, "KPI")

        except Exception as err:
            raise MLOpsException(err)

    def _save_df_to_output(self, output_path, dfs_dict, type_name):
        """
         The method save dataframes to the output path.

         :param self:
         :param output_path: Output path
         :param dfs_dict: dict of dfs to save
         :param type_name: A string of the type of DF
         :return:
         """
        for file_name in dfs_dict.keys():
            dfs_dict[file_name].to_csv(output_path + "/" + str(file_name) + '_' +
                                       type_name + '.csv')

    def get_sys_stat_df_file(self):
        """
        Get the stats dataframe per file

        :return: stats datafame
        :rtype: dict of pandas Dataframe
        """
        return self._sys_stat_df_file

    def get_aggregate_df_file(self):
        """
        Get the aggregation report dataframe per file

        :return: aggregate datafame
        :rtype: dict of pandas Dataframe
        """
        return self._aggregate_df_file

    def get_file_names(self):
        """
        Get the file names of the time capture

        :return: file names list
        :rtype: list
        """
        return self._file_names

    def get_attribute_names_list(self):
        """
        Get the names of variables in the time capture

        :return: names list
        :rtype: list
        """
        return self._attribute_names_list

    def get_matrix_df_file(self):
        """
        Get the matrix dataframe per file

        :return: matrix datafame
        :rtype: dict of pandas Dataframe
        """
        return self._matrix_df_file

    def get_multigraph_df_file(self):
        """
        Get the multi-graph dataframe per file

        :param: self:
        :return: multigraph datafame
        :rtype: dict of pandas Dataframe
        """
        return self._multigraph_df_file

    def get_events(self, type=None, mlapp_node=None, agent=None, start_time=None, end_time=None,
                   is_alert=True, api_test_mode=False):
        """
        Fetch events according to a time window and other filters.

        :param type: Event type
        :param mlapp_node: Set the mlapp_node to select events from. If None, events will be fetched
         for the entire MLApp. not valid
        :param agent: (optional) Either an agent ID or an agent object. If specified, the query
         will filter by agent. not valid
        :param start_time: (optional) Filter by start time, this should be a datetime object.
        :param end_time: (optional) Filter by end time, this should be a datetime object.
        :param is_alert: (optional) Filter by is_alert - between alerts and events. Default
         is True. If None is provided all events are fetched. not valid
        :param api_test_mode:
        :return: A dataframe containing information about the events obtained.

        :Example:

        >>> from parallelm.mlops.mlops import mlops as pm
        >>> from datetime import datetime, timedelta
        >>> now = datetime.utcnow()
        >>> hour_ago = (now - timedelta(hours=1))
        >>> pm.init(mlops_mode="time_capture", input_file="tarfile_path")
        >>> df = pm.get_events(start_time=hour_ago, end_time=now)
        >>> pm.done()

        """

        df = self._events_df_file
        start_time_ms_timestamp = \
            self._time_functions.time_to_str_timestamp_milli(start_time) if start_time is not None else None
        end_time_ms_timestamp = \
            self._time_functions.time_to_str_timestamp_milli(end_time) if end_time is not None else None

        if (start_time_ms_timestamp is not None) & (end_time_ms_timestamp is not None):
            df = self._time_functions.time_filter(df, start_time_ms_timestamp, end_time_ms_timestamp)
        return df

    def get_stats(self, name, mlapp_node=None, agent=None, start_time=None, end_time=None,
                  no_of_lines=-1, api_test_mode=False):
        """
        Get statistics.
        The get_stats() call allows access to statistics from each of the MLApp components;
        The start_time and end_time can be used to define a time window for the returned stats.

        :param name: The name of the statistic - this is the name used in the stats method
        :param mlapp_node: The name of the MLApp node. Not valid
        :param agent: ID of the agent which collected the statistics Not valid
        :param start_time: A datetime object representing the start of the time window
        :param end_time: A datetime object representing the end of the time window
        :param no_of_lines: deprecated
        :param api_test_mode:
        :return: A dataframe representing the statistics collected over time

        :Example:

        >>> from parallelm.mlops.mlops import mlops as pm
        >>> from datetime import datetime, timedelta
        >>> now = datetime.utcnow()
        >>> hour_ago = (now - timedelta(hours=1))
        >>> pm.init(mlops_mode="time_capture", input_file="tarfile_path")
        >>> df = pm.get_stats(name="name", start_time=hour_ago, end_time=now)
        >>> pm.done()

        The above example assumes the MLApp this code is running as part of has a component
         called "my_training".
        1. The code obtains the list of agents which are used by the "my_training" component.
        2. It computes the timestamp of now.
        3. It computes the timestamp of an hour ago.
        4. It calls the get_stats method asking for the "myValue" statistic from the first agent.
        The return value is a dataframe object.


        """
        df = self._df_name[name]
        start_time_ms_timestamp = \
            self._time_functions.time_to_str_timestamp_milli(start_time) if start_time is not None else None
        end_time_ms_timestamp = \
            self._time_functions.time_to_str_timestamp_milli(end_time) if end_time is not None else None
        if (start_time_ms_timestamp is not None) & (end_time_ms_timestamp is not None):
            df = self._time_functions.time_filter(df, start_time_ms_timestamp, end_time_ms_timestamp)
        return df

    def get_kpi(self, name, start_time, end_time):
        """
        Return KPI statistics withina given time window

        :param name: statistic name
        :param start_time: start of time window
        :param end_time: end of time window
        :return: Dataframe with the KPI statistics values
        """
        return self.get_stats(name, mlapp_node=None, agent=None, start_time=start_time,
                              end_time=end_time)

    def get_mlapp_id(self):
        """
        Get the current MLApp id.
        For example: "0de31941-76f9-40af-a71f-5d5e10298176"

        :return: MLApp id
        :rtype: string

        """
        return self._mlapp_id

    def get_mlapp_name(self):
        """
        Get the current MLApp name
        Currently not available

        :return: MLApp name
        :rtype: string
        :raises: MLOpsException
        """
        raise MLOpsException("get_mlapp_name is not available")

    def get_mlapp_policy(self):
        """
        Get the current MLApp policy
        For example: "ALWAYS_UPDATE" is a policy to always propagate new models between pipeline
         producers and pipeline consumers.

        :return: MLApp policy.
        :rtype: string

        """
        return self._model_policy

    def get_nodes(self):
        """
        Return a list of all the MLAppNode objects that are part of the current MLApp

        :return: list of MLAppNodes

        """
        return self._nodes_id

    def get_node(self, name):
        """
        Return the MLAppNode object that corresponds to the given name.
        Currently not available

        :param name: name of component to look for
        :return: MLAppNode object if found; otherwise, None
        :raises: MLOpsException

        """
        raise MLOpsException("get_node is not available")

    def get_current_node(self):
        """
        Return the current node
        Currently not available

        :return: the current node
        :raises: MLOpsException
        """

        raise MLOpsException("get_current_node is not available")

    def get_agents(self, node_name):
        """
        Return a list of Agent objects corresponding to the agents used by this component.
        Currently not available

        :param node_name: return only agents running this node
        :return: list of agents
        :raises: MLOpsException

        """

        raise MLOpsException("get_agents is not available")

    def get_agent(self, node_name, agent_id):
        """
        Return the agent with the corresponding agent id.
        Currently not avaiable

        :param node_name: return an agent running this node
        :param agent_id: the id of the agent
        :return: the agent
        :raises: MLOpsException
        """

        raise MLOpsException("get_agent is not available")

    def get_data_distribution_stats(self, model_id, api_test_mode=False):
        """
        Return a dataframe with statistics for the given model ID.
        Currently not available


        :param self:
        :param model_id:
        :param api_test_mode:
        :return:
        """

        raise MLOpsException("get_data_distribution_stats is not available")

    def get_models_by_time(self, start_time, end_time, download=False, pipeline_name=None):
        """
        Retrieve models in the context of the current MLApp from MLOps based on start and end times.
        Currently not active

        :param start_time: a datetime object specifying window start time
        :type start_time: datetime
        :param end_time: a datetime object specifying window end time
        :type end_time: datetime
        :param download: If true, download the model data and provide it as an additional column
         in the dataframe
        :type download: bool
        :param pipeline_name: query by pipeline
        :type pipeline_name: string
        :return: Spark or pandas dataframe based on mode with the models as a byte array
        """

        raise MLOpsException("get_models_by_time is not available")

    def get_model_by_id(self, model=0, download=False, api_test_mode=False):
        """
        Return the model with this id
        Currently not active

        :param model: the model id
        :param download: Boolean, whether to download the model
        :param api_test_mode:
        :return: return model as a bytearray
        :raises: MLOpsException
        """

        raise MLOpsException("get_model_by_id is not available")

    def done(self):
        pass
