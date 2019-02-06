
from parallelm.mlops.channels.mlops_python_channel import MLOpsPythonChannel
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.mlops_env_constants import MLOpsEnvConstants
from parallelm.protobuf.ReflexEvent_pb2 import ReflexEvent

import json
import sys
import os

import logging


class FileChannelOutputFormat:
    CSV = "csv"
    JSON = "json"


class FileChannel(MLOpsPythonChannel):
    def __init__(self, file_path=None, file_handle=None, output_fmt=FileChannelOutputFormat.CSV):
        logging.basicConfig()
        self._f = sys.stdout
        self._using_default_stdout = True
        self._logger = logging.getLogger(__name__)
        self._output_fmt = output_fmt

        if (file_path is not None) and (file_handle is not None):
            raise MLOpsException("Both file_path and file_hndl are not none - can not decide")

        if file_path:
            self._logger.info("Using provided file path: {}".format(file_path))
            self._f = open(file_path, "w")
            self._using_default_stdout = False
        elif file_handle:
            self._logger.info("Using provided file handle")
            self._f = file_handle
            self._using_default_stdout = False
        elif MLOpsEnvConstants.PM_MLOPS_FILE in os.environ:
            file_path = os.environ[MLOpsEnvConstants.PM_MLOPS_FILE]
            self._logger.info("Using File Channel file from environment: {}".format(file_path))
            self._f = open(file_path, "w")
            self._using_default_stdout = False
        else:
            self._logger.info("Could not detect env variable: {} , using default file {}".format(
                MLOpsEnvConstants.REST_SERVER_PORT, "stdout"))

    def _stat_object(self, mlops_stat):
        stat_str = ""
        if self._output_fmt == FileChannelOutputFormat.CSV:
            stat_str = mlops_stat.to_csv_line()
        elif self._output_fmt == FileChannelOutputFormat.JSON:
            stat_str = mlops_stat.to_json()

        self._f.write("{}\n".format(stat_str))
        self._f.flush()

    def stat_object(self, mlops_stat, reflex_event_message_type=ReflexEvent.StatsMessage):
        self._stat_object(mlops_stat)

    def done(self):
        if not self._using_default_stdout:
            self._f.close()

    def event(self, event):
        event_str = ""
        if self._output_fmt == FileChannelOutputFormat.CSV:
            event_str = "{}, {}, {}, {}".format(event.eventType, event.eventLabel, event.isAlert, event.data)

        elif self._output_fmt == FileChannelOutputFormat.JSON:
            event_dict = {
                'eventType': event.eventType,
                'eventLabel': event.eventLabel,
                'isAlert': event.isAlert,
                'data': event.data
            }

            event_str = json.dumps(event_dict)

        self._f.write("{}\n".format(event_str))
        self._f.flush()

    def feature_importance(self, feature_importance_vector=None, feature_names=None, model=None, df=None):

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
