"""
mlhealth library - provides the base routines to fetch data from the DB and convert to
python structures
"""

import json
import os
import pandas as pd
from kazoo.client import KazooClient
from parallelm.mlops.constants import Constants
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.mlops_mode import MLOpsMode
from parallelm.mlops.mlops_rest_factory import MlOpsRestFactory


class MLHealth(object):
    """
    This class provides APIs to access health metrics created for a workflow instance.
    It includes stats and models.
    """

    def __init__(self):
        self._sc = None
        self._eco_server = None
        self._eco_port = None
        self._db_host = None
        self._db_port = None
        self._mode = None
        self._wf_id = None
        self._agent_list = []
        self._jvm_mlops = None
        self._rest_helper = None
        self._zk_host = None
        self._token = None

    @staticmethod
    def _search_list_dict(kv, key, value):
        for x in kv:
            if x[key] == value:
                return x
        return None

    def init(self, sc=None, wf_id=None, eco_server=None, eco_port=None, db_host=None,
             db_port=None, zk_host=None, token=None, mode=MLOpsMode.PYSPARK):
        """
        Perform initialization of the health library. eco and db configuration can be
        set up using environment variables and hence are optional. This is true for the workflow
        instance id as well. Currently, python and pyspark mode of operation are supported.

        :param sc: optional spark context for pyspark jobs
        :param wf_id: workflow instance id
        :param eco_server: eco server host
        :param eco_port: eco server port
        :param db_host: stats db host
        :param db_port: stats db port
        :param zk_host: zookeeper host port string
        :param token: authentication token
        :param mode: python or pyspark
        :return:
        """
        self._sc = sc

        no_zk = False
        if zk_host is None:
            if os.environ.get(Constants.MLOPS_ZK_HOST) is not None:
                self._zk_host = os.environ[Constants.MLOPS_ZK_HOST]
            else:
                no_zk = True
        else:
            self._zk_host = zk_host

        if token is None:
            if os.environ.get(Constants.MLOPS_TOKEN) is not None:
                self._token = os.environ[Constants.MLOPS_TOKEN]
            else:
                raise MLOpsException("Internal Error: No auth token provided")
        else:
            self._token = token

        if no_zk is False:
            # initialize zk connections and get active eco server
            try:
                zk = KazooClient(hosts=self._zk_host, read_only=True)
                zk.start()
                if zk.exists('/ECO/curator/activeHostPort'):
                    data, stat = zk.get("/ECO/curator/activeHostPort")
                    eco_host_port = data.decode("utf-8").split(':')

                    if len(eco_host_port) is 2:
                        self._eco_server = eco_host_port[0]
                        self._eco_port = eco_host_port[1]
                    else:
                        raise MLOpsException("Internal Error: Invalid zookeeper active server "
                                             "entry")
                else:
                    raise MLOpsException("Unable to connect to the active MLOps server")
                zk.stop()
            except Exception:
                raise MLOpsException("Unable to locate active MLOps server")

        # if eco server was found using zookeeper, then don't use the environment variable
        if self._eco_server is None:
            if eco_server is None:
                if os.environ.get(Constants.MLOPS_ECO_HOST) is not None:
                    self._eco_server = os.environ[Constants.MLOPS_ECO_HOST]
                else:
                    raise MLOpsException("MLOps server host not provided")
            else:
                self._eco_server = eco_server

        if self._eco_port is None:
            if eco_port is None:
                if os.environ.get(Constants.MLOPS_ECO_PORT) is not None:
                    self._eco_port = os.environ[Constants.MLOPS_ECO_PORT]
                else:
                    raise MLOpsException("MLOps server port not provided")
            else:
                self._eco_port = eco_port

        if db_host is None:
            if os.environ.get(Constants.MLOPS_TIMESERIES_DB_HOST) is not None:
                self._db_host = os.environ[Constants.MLOPS_TIMESERIES_DB_HOST]
            else:
                raise MLOpsException("Database server host not provided")
        else:
            self._db_host = db_host

        if db_port is None:
            if os.environ.get(Constants.MLOPS_TIMESERIES_DB_PORT) is not None:
                self._db_port = os.environ[Constants.MLOPS_TIMESERIES_DB_PORT]
            else:
                raise MLOpsException("Database server port not provided")
        else:
            self._db_port = db_port

        if wf_id is None:
            if os.environ.get(Constants.MLOPS_HEALTH_WF_ID) is not None:
                self._wf_id = os.environ[Constants.MLOPS_HEALTH_WF_ID]
            else:
                raise MLOpsException("{} instance id not provided".format(Constants.ION_LITERAL))
        else:
            self._wf_id = wf_id

        self._mode = mode

        self._rest_helper = MlOpsRestFactory().get_rest_helper(MLOpsMode.STAND_ALONE)
        self._rest_helper.init(self._eco_server, self._eco_port, self._token)

        if sc is not None:
            try:
                import pyspark
                import pyspark.mllib.common as ml
                from pyspark.sql import SQLContext
                import pyspark.sql.types

                if not isinstance(sc, pyspark.context.SparkContext):
                    raise MLOpsException("sc argument is not pyspark context")

                # initialize jvm to mlops
                self._jvm_mlops = sc._jvm.org.mlpiper.mlops.MLOps
                ping_val = 5
                ping_ret = self._jvm_mlops.ping(ping_val)
                if ping_ret != 5:
                    raise MLOpsException(
                        "Got unexpected value from MLOps.ping sent {} got {} ".format(ping_val, ping_ret))
            except Exception:
                err = "Unable to access MLOps objects within the health program"
                self._jvm_mlops = None
                raise MLOpsException(err)

        groups = self._rest_helper.get_groups()
        agents = self._rest_helper.get_agents()

        # based on ion / workflow run id, get ion description
        wf_instances = self._rest_helper.get_workflow_instances()

        wfi = MLHealth._search_list_dict(wf_instances, 'id', self._wf_id)
        if wfi is None:
            raise MLOpsException("Could not locate {} instance {}".format(Constants.ION_LITERAL,
                                                                          self._wf_id))

        # get agents/groups
        node_info = wfi['pipelineInstanceIdToWfNode']

        for id in node_info:
            kv = node_info[id]
            agents_in_group = MLHealth._search_list_dict(groups, 'id', kv['groupId'])['agents']
            if agents_in_group is None:
                raise MLOpsException("Could not locate group {}".format(kv['groupId']))

            ptype = kv['pipelineType']

            agent_addrs = []

            for aig in agents_in_group:
                agent_addr = MLHealth._search_list_dict(agents, 'id', aig)
                if agent_addr is None:
                    raise MLOpsException("Could not locate agent {} in group description".format(aig))

                agent_addrs.append(agent_addr['address'])
            self._agent_list.append((id, ptype, agent_addrs))

    def _get_sparksql_context(self):
        from pyspark.sql import SQLContext
        return SQLContext.getOrCreate(self._sc)

    def _convert_pd_to_df(self, pd_df):
        sql_context = self._get_sparksql_context()
        return sql_context.createDataFrame(pd_df)

    def _get_model_pdf(self):
        models = self._rest_helper.get_model_list()
        return pd.read_json(json.dumps(models))

    def _get_model_python(self, start_time, end_time):
        mdf = self._get_model_pdf()

        newdf = mdf[(mdf['workflowRunId'] == self._wf_id) &
                    (mdf['createdTimestamp'] >= start_time) & (mdf['createdTimestamp'] <= end_time)]
        if newdf.shape[0] == 0:
            raise MLOpsException("No models found in time range {}:{} for instance {}".format(
                start_time, end_time, self._wf_id))

        output_df = newdf[['createdTimestamp', 'name', 'id']]

        vals = newdf['id'].values
        models = self._rest_helper.get_model_by_id(vals)

        output_df = output_df.assign(model=models)
        return output_df

    def _get_model_pyspark(self, start_time, end_time):
        df = self._get_model_python(start_time, end_time)
        sdf = self._convert_pd_to_df(df)

        return sdf

    def get_model(self, start_time, end_time):
        """
        Retrieve models from eco server based on start and end times. Use the workflow
        instance id to retrieve only models that are specific to this job.

        :param start_time: start time in milliseconds
        :param end_time: end time in milliseconds
        :return: spark or pandas dataframe based on mode with the models as a byte array
        """
        if self._mode == MLOpsMode.PYSPARK:
            return self._get_model_pyspark(start_time, end_time)
        elif self._mode == MLOpsMode.PYTHON:
            return self._get_model_python(start_time, end_time)
        else:
            raise MLOpsException("Invalid mode: [{}]".format(self._mode))
