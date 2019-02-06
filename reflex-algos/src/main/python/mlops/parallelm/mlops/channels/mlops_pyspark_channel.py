import json
import os
import socket

import pyspark
import pyspark.mllib.common as ml
from google.protobuf.internal import encoder
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import DataFrame

from parallelm.mlops.channels.mlops_channel import MLOpsChannel
from parallelm.mlops.constants import Constants
from parallelm.mlops.data_to_json import DataToJson
from parallelm.mlops.mlops_env_constants import MLOpsEnvConstants
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats_category import StatCategory, StatGraphType


def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")


class MLOpsPySparkChannel(MLOpsChannel):
    def __init__(self, sc):
        self._jvm_mlops = None
        self._logger = None
        self._sc = sc
        self._socket = None

        try:
            if not isinstance(sc, pyspark.context.SparkContext):
                raise MLOpsException("sc argument is not pyspark context")

            self._jvm_mlops = sc._jvm.com.parallelmachines.mlops.MLOps
            ping_val = 5
            ping_ret = self._jvm_mlops.ping(ping_val)
            if ping_ret != 5:
                raise MLOpsException(
                    "Got unexpected value from MLOps.ping sent {} got {} ".format(ping_val, ping_ret))

            self._log4j_logger = self._sc._jvm.org.apache.log4j
            self._logger = self._log4j_logger.LogManager.getLogger(__name__)

        except Exception as e:
            err = "Unable to access com.parallelmachines.mlops.MLOps object via jvm context"
            print(e)
            raise MLOpsException(err)

        rest_server_port = Constants.REST_SERVER_DEFAULT_PORT
        if MLOpsEnvConstants.REST_SERVER_PORT in os.environ:
            rest_server_port = int(os.environ[MLOpsEnvConstants.REST_SERVER_PORT])
            self._logger.info("Got server port from environment: {}".format(rest_server_port))
        else:
            self._logger.info("Could not detect env variable: {} , using default port {}".format(
                MLOpsEnvConstants.REST_SERVER_PORT, rest_server_port))

        self._init_events_client()

        wait_for_exit = True
        if MLOpsEnvConstants.REST_SERVER_WAIT_FOR_EXIT in os.environ:
            wait_for_exit = str2bool(os.environ[MLOpsEnvConstants.REST_SERVER_WAIT_FOR_EXIT])

        self._jvm_mlops.init(sc._jsc, True, rest_server_port, wait_for_exit)

    def get_logger(self, name):
        return self._log4j_logger.LogManager.getLogger(name)

    def done(self):
        self._jvm_mlops.done()

    def stat(self, name, data, modelId, category=None, model=None, model_stat=None):
        if category in (StatCategory.CONFIG, StatCategory.TIME_SERIES):
            self._logger.info("{} stat called: name: {} data_type: {} class: {}".
                              format(Constants.OFFICIAL_NAME, name, type(data), category))

            stat_mode, graph_type = self.resolve_type(data, category)

            self._jvm_mlops.stat(name, data, modelId, graph_type, stat_mode)
        elif category is StatCategory.INPUT:
            if model_stat:
                hist_rdd = self._sc.parallelize(model_stat)
            else:
                hist_rdd = self._sc.emptyRDD()

            if isinstance(data, pyspark.rdd.PipelinedRDD):
                self._jvm_mlops.inputStatsFromRDD(name, modelId, ml._py2java(self._sc, data),
                                                  ml._py2java(self._sc, hist_rdd))
            elif isinstance(data, pyspark.sql.DataFrame):

                try:
                    self._logger.info("Spark ML is provided to help MCenter calculate Health!")
                    if model:
                        spark_ml_model = model._to_java()
                        self._jvm_mlops.inputStatsFromDataFrame(name,
                                                                modelId,
                                                                ml._py2java(self._sc, data),
                                                                ml._py2java(self._sc, hist_rdd),
                                                                spark_ml_model)
                    else:
                        self._jvm_mlops.inputStatsFromDataFrame(name,
                                                                modelId,
                                                                ml._py2java(self._sc, data),
                                                                ml._py2java(self._sc, hist_rdd))

                except Exception as e:
                    self._logger.info("model does not seem to have _to_java method \n{}".format(e))
                    raise MLOpsException("Unable to convert from python to java {}".format(e))
            else:
                raise MLOpsException("Statistic type {} is not supported for data type {}".
                                     format(StatCategory.INPUT, type(data)))
        else:
            raise MLOpsException("stat_class: {} not supported yet".format(category))

    def table(self, name, tbl_data):
        tbl_data_json = DataToJson.json(tbl_data, StatGraphType.MATRIX)
        tbl_data_json_str = json.dumps(tbl_data_json)
        self._jvm_mlops.statTable(name, tbl_data_json_str)

    def stat_object(self, mlops_stat):

        self._jvm_mlops.statJSON(mlops_stat.name,
                                 mlops_stat.data_to_json(),
                                 mlops_stat.model_id,
                                 mlops_stat.graph_type,
                                 mlops_stat.mode,
                                 mlops_stat.stat_type,
                                 mlops_stat.timestamp_ns_str())

    def _init_events_client(self):
        if MLOpsEnvConstants.MLOPS_ML_OBJECT_SERVER_HOST not in os.environ:
            self._logger.info("Missing env var for ml-object (events) server host! Skipping initialization!")
            return

        if MLOpsEnvConstants.MLOPS_ML_OBJECT_SERVER_PORT not in os.environ:
            self._logger.info("Missing env var for ml-object (events) server port! Skipping initialization!")
            return

        server_host = os.environ[MLOpsEnvConstants.MLOPS_ML_OBJECT_SERVER_HOST]
        server_port = int(os.environ[MLOpsEnvConstants.MLOPS_ML_OBJECT_SERVER_PORT])
        self._logger.info("Got ml-object (events) server url: {}:{}".format(server_host, server_port))

        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.connect((server_host, server_port))
        except socket.error as e:
            self._sock_cleanup_and_raise(e)

    def event(self, event):
        if self._socket:
            self._write_delimited_to(event)

    def _write_delimited_to(self, event):
        serialized_message = event.SerializeToString()
        delimiter = encoder._VarintBytes(len(serialized_message))

        try:
            self._socket.sendall(delimiter + serialized_message)
        except socket.error as e:
            self._sock_cleanup_and_raise(e)

    def _sock_cleanup_and_raise(self, ex):
        if self._socket:
            self._socket.close()
            self._socket = None
        raise MLOpsException(str(ex))

    def feature_importance(self, feature_importance_vector=None, feature_names=None, model=None, df=None):

        self._validate_specific_importance_inputs(model, df)

        # Get the feature importance vector
        if feature_importance_vector:
            feature_importance_vector_final = feature_importance_vector
        else:
            try:
                last_stage = model.stages[-1]
                feature_importance_vector_final = last_stage.featureImportances
            except Exception as e:
                raise MLOpsException("Got an exception:{}".format(e))

        if feature_names:
            important_named_features = [[name, feature_importance_vector_final[imp_idx]]
                                        for imp_idx, name in enumerate(feature_names)]
            return important_named_features
        else:
            try:
                # The code below gets the names of the features from the dataframe. This information exists
                # in the metadata of the dataframe and specifically in the ml attributes of the features column
                # metadata.
                # TODO: use model.getFeatureCol to get the general name of the feature column instead of 'features'
                struct_field = df.schema["features"]
                important_named_features = []

                # get the elements name according to the index of the featureImportance vector
                for importance_index, importance_elements in enumerate(feature_importance_vector_final):
                    feature_name = ''
                    for features_family in struct_field.metadata["ml_attr"]["attrs"]:
                        for features in struct_field.metadata["ml_attr"]["attrs"][str(features_family)]:
                            if features["idx"] == importance_index:
                                feature_name = str(features["name"])
                    important_named_features.append([feature_name, importance_elements])
                return important_named_features
            except Exception as e:
                raise MLOpsException("Got an exception:{}".format(e))

    def _validate_specific_importance_inputs(self, model, df):
        """

        :param model: sparkML pipeline model
        :param df: sparkML dataframe
        :return:
        """

        if PipelineModel is not None and not isinstance(model, PipelineModel):
            raise MLOpsException("Got an Exception. should be a pipeline model")

        if df is not None and not isinstance(df, DataFrame):
            raise MLOpsException("Got an Exception. should be a sparkML dataframe")
