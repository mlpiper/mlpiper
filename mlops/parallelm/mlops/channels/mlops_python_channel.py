import logging
import os
import socket

import numpy as np
import pandas as pd
from google.protobuf.internal import encoder
from numpy.core.multiarray import ndarray

from parallelm.mlops.channels.mlops_channel import MLOpsChannel
from parallelm.mlops.channels.python_channel_health import PythonChannelHealth
from parallelm.mlops.constants import Constants
from parallelm.mlops.mlops_env_constants import MLOpsEnvConstants
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats.single_value import SingleValue
from parallelm.mlops.stats_category import StatCategory
from parallelm.protobuf.ReflexEvent_pb2 import ReflexEvent


class MLOpsPythonChannel(MLOpsChannel):
    def __init__(self):
        logging.basicConfig()
        self._logger = logging.getLogger(__name__)
        self._socket = None
        self._init_events_client()

    def get_logger(self, name):
        pass

    def done(self):
        pass

    def stat(self, name, data, model_id, category=None, model=None, model_stat=None):

        if category in (StatCategory.CONFIG, StatCategory.TIME_SERIES):
            self._logger.debug("{} stat called: name: {} data_type: {} class: {}".
                               format(Constants.OFFICIAL_NAME, name, type(data), category))

            single_value = SingleValue().name(name).value(data).mode(category)
            self.stat_object(single_value.get_mlops_stat(model_id))

        elif category is StatCategory.INPUT:
            self._logger.info("{} stat called: name: {} data_type: {} class: {}".
                              format(Constants.OFFICIAL_NAME, name, type(data), category))

            # right now, only for ndarray or dataframe, we can generate input stats.
            if isinstance(data, ndarray):
                dims = data.shape

                feature_length = 1
                try:
                    feature_length = dims[1]
                except Exception as e:
                    data = data.reshape(dims[0], -1)
                    pass

                array_of_features = []
                for i in range(feature_length):
                    array_of_features.append("c" + str(i))

                features_values = data
                features_names = array_of_features

            elif isinstance(data, pd.DataFrame):
                features_values = np.array(data.values)
                features_names = list(data.columns)

            else:
                raise MLOpsException("stat_class: {} not supported yet for data: {}".format(category, data))

            self._logger.info("model_stat is: {}".format(model_stat))

            PythonChannelHealth.generate_health_and_heatmap_stat(stat_object_method=self.stat_object,
                                                                 logger=self._logger,
                                                                 features_values=features_values,
                                                                 features_names=features_names,
                                                                 model_stat=model_stat,
                                                                 model_id=model_id,
                                                                 num_bins=13)
        else:
            raise MLOpsException("stat_class: {} not supported yet".format(category))

    def stat_object(self, mlops_stat, reflex_event_message_type=ReflexEvent.StatsMessage):
        evt = ReflexEvent()
        evt.eventType = reflex_event_message_type

        stat_str = str(mlops_stat.to_semi_json()).encode("utf-8")
        evt.data = stat_str

        self._logger.debug("sending: {}".format(stat_str))
        self._write_delimited_to(evt)

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
            msg = "Error connecting to server {}:{}".format(server_host, server_port)
            self._logger.error(msg)
            self._sock_cleanup_and_raise(e, msg)

        self._logger.info("Done init event client")

    def table(self):
        raise MLOpsException("Not implemented")

    def event(self, event):
        if self._socket:
            self._write_delimited_to(event)

    def _write_delimited_to(self, event):
        serialized_message = event.SerializeToString()
        delimiter = encoder._VarintBytes(len(serialized_message))
        if self._socket:
            try:
                self._socket.sendall(delimiter + serialized_message)
            except socket.error as e:
                self._sock_cleanup_and_raise(e)

    def _sock_cleanup_and_raise(self, ex, msg=""):
        if self._socket:
            self._socket.close()
            self._socket = None
        raise MLOpsException(str(ex) + msg)

    def feature_importance(self, feature_importance_vector=None, feature_names=None, model=None, df=None):

        self._validate_specific_importance_inputs(df)

        # Get the feature importance vector
        if feature_importance_vector:
            feature_importance_vector_final = feature_importance_vector
        else:
            try:
                feature_importance_vector_final = model.feature_importances_
            except Exception as e:
                raise MLOpsException("Got an exception:{}".format(e))

        if feature_names:
            important_named_features = [[name, feature_importance_vector_final[imp_idx]] for imp_idx,name in enumerate(feature_names)]
            return important_named_features
        else:
            try:
                feature_names = df.columns[1:]
                important_named_features = [[name, feature_importance_vector_final[imp_idx]] for imp_idx,name in enumerate(feature_names)]
                return important_named_features
            except Exception as e:
                raise MLOpsException("Got an exception:{}".format(e))

    def _validate_specific_importance_inputs(self, df):
        """

        :param df: pandas dataframe
        :return:
        """

        if df is not None and not isinstance(df, pd.DataFrame):
            raise MLOpsException("Got an Exception. should be a pandas dataframe")
