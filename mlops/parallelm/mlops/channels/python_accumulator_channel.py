
from parallelm.mlops.channels.mlops_python_channel import MLOpsPythonChannel
from parallelm.protobuf.ReflexEvent_pb2 import ReflexEvent

import logging


class PythonAccumulatorChannel(MLOpsPythonChannel):
    def __init__(self, rest_helper=None, pipeline_inst_id=None):
        super(PythonAccumulatorChannel, self).__init__(rest_helper, pipeline_inst_id)
        self._rest_helper = rest_helper
        self._pipeline_inst_id = pipeline_inst_id
        logging.basicConfig()
        self._logger = logging.getLogger(__name__)
        self._stats_accumulator = {}

    def stat_object(self, mlops_stat, reflex_event_message_type=ReflexEvent.StatsMessage):
        self._logger.debug("Stat object received {}".format(mlops_stat.name))
        self._stats_accumulator[mlops_stat.name] = mlops_stat.to_semi_json()
        self._logger.debug("Storing following keys {}".format(self._stats_accumulator.keys()))

    def get_stats_map(self):
        self._logger.debug("Retrieving keys {}".format(self._stats_accumulator.keys()))
        return self._stats_accumulator

    def event(self, event):
        if self._rest_helper:
            super(PythonAccumulatorChannel, self).event(event)
