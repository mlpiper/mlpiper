"""
MLOps support for scalar statistics.
"""
import six

from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats_category import StatGraphType, StatsMode
from parallelm.mlops.stats.mlops_stat import MLOpsStat
from parallelm.mlops.stats.mlops_stat_getter import MLOpsStatGetter
from parallelm.protobuf import InfoType_pb2


class SingleValue(MLOpsStatGetter):
    """
    Scalar statistics have type :class:`StatsMode`
    """

    def __init__(self):
        self._name = "SingleValue"
        self._value = 0
        self._mode = StatsMode.TimeSeries

    def mode(self, mode):
        self._mode = mode
        return self

    def name(self, name):
        """
        Set the name of the object

        :param name: value name
        :return: self
        """
        self._name = name
        return self

    def value(self, value):
        """
        Set the labels for this graph

        :param label_list: List of strings to use as labels
        :return: self
        """
        if not isinstance(value, (six.integer_types, float)):
            raise MLOpsException("Value can be either int of float")

        self._value = value
        return self

    def _to_dict(self):
        dd = {
            self._name: self._value
        }
        return dd

    def get_mlops_stat(self, model_id):

        data = self._to_dict()

        mlops_stat = MLOpsStat(name=self._name,
                               stat_type=InfoType_pb2.General,
                               graph_type=StatGraphType.LINEGRAPH,
                               mode=StatsMode.TimeSeries,
                               data=data,
                               model_id=model_id)
        return mlops_stat
