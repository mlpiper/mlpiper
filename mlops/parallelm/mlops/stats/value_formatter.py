from parallelm.mlops.stats_category import StatGraphType
from parallelm.mlops.mlops_exception import MLOpsException


class ValueFormatter:
    """
    This class is used to construct a unique pattern from the keys & values, according to the given graph type.
    """

    def __init__(self, key, value, graph_type):
        self._key = key
        self._value = value
        self._graph_type = graph_type

    def value(self):
        if self._graph_type == StatGraphType.LINEGRAPH:
            return {self._key: self._value}
        else:
            raise MLOpsException("Value graph type is not supported! key: {}, value: {}, graph_type: {}"
                                   .format(self._key, self._value, self._graph_type))

