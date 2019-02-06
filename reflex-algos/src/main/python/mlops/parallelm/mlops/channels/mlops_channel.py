"""
Class representing the output channel to be used to report statistics/operations and such
"""

import abc
from parallelm.mlops.stats_category import StatCategory, StatsMode, StatGraphType
from parallelm.mlops.stats.table import is_list_of_lists

class MLOpsChannel:
    """
    The MLOpsChannel is providing the way to communicate with the MLOPs system.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def done(self):
        pass

    @abc.abstractmethod
    def stat(self, name, data, model_id, category=None, sparkml_model=None, model_stat=None):
        """
        methods to submit a stat to a specific channel (pyspark/python/file etc) each channel is
        required to implement the method to support posting of statistics to that channel

        :param name: name of statistic
        :param data: value of statistic (json)
        :param category: category (input, time_series, config)
        :param sparkml_model: sparkml model object produced during training
        :param model_stat: during inference, histogram comparisons require training-model stats (
        histograms) - this is pre-fetched by mlops and provided to the channel as required to
        perform the comparison
        :return:
        """
        pass

    @abc.abstractmethod
    def table(self, name, tbl_data):
        pass

    @abc.abstractmethod
    def stat_object(self, stat_object):
        """
        A method to output a stat object
        :param stat_object:
        :return:
        """
        pass

    @abc.abstractmethod
    def event(self, event):
        """
        A method to report an event object
        :param event: Event object to report, should be of type ReflexEvent.
        :return:
        """

    def resolve_type(self, data, category):
        graph_type = StatGraphType.LINEGRAPH
        if category == StatCategory.CONFIG:
            stat_mode = StatsMode.Instant
            if is_list_of_lists(data):
                stat_type = StatGraphType.MATRIX
        else:
            stat_mode = StatsMode.TimeSeries
        return stat_mode, graph_type
