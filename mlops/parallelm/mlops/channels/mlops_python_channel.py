import logging

import numpy as np
import pandas as pd
from numpy.core.multiarray import ndarray

from parallelm.mlops.channels.mlops_channel import MLOpsChannel
from parallelm.mlops.channels.python_channel_health import PythonChannelHealth
from parallelm.mlops.constants import Constants
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats.single_value import SingleValue
from parallelm.mlops.stats_category import StatCategory
from parallelm.protobuf.ReflexEvent_pb2 import ReflexEvent
from google.protobuf.json_format import MessageToJson


class MLOpsPythonChannel(MLOpsChannel):
    def __init__(self, rest_helper, pipeline_inst_id):
        logging.basicConfig()
        self._logger = logging.getLogger(__name__)
        self._rest_helper = rest_helper
        self._pipeline_inst_id = pipeline_inst_id

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
        stat_str = str(mlops_stat.to_semi_json()).encode("utf-8")

        self._logger.debug("sending: {}".format(stat_str))
        try:
            self._rest_helper.post_stat(self._pipeline_inst_id, stat_str)
        except Exception as e:
            self._logger.error("Fail to post stat - " + str(e))
            pass

    def table(self):
        raise MLOpsException("Not implemented")

    def event(self, event):
        stat_str = str(MessageToJson(event)).encode("utf-8")
        self._logger.debug("sending: {}".format(stat_str))
        self._rest_helper.post_event(self._pipeline_inst_id, stat_str)

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
