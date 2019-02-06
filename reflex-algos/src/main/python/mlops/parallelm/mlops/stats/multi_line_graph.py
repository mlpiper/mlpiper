from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats_category import StatGraphType, StatsMode
from parallelm.mlops.stats.mlops_stat import MLOpsStat
from parallelm.mlops.stats.mlops_stat_getter import MLOpsStatGetter
from parallelm.protobuf import InfoType_pb2
import copy
import six


class MultiLineGraph(MLOpsStatGetter):
    """
    A container for multi-line graph data. Using this container, you can report a single value for several data points
    per run that will be displayed (and saved) together. Each pipeline run accumulates points to the graph.

    :Example:

    >>> mlt = MultiLineGraph().name("MultiLine").labels(["l1", "l2", "l3"])
    >>> mlt.data([11, 23, 5])
    >>> mlops.set_stat(mlt)

    This example will generate a multi-line graph object "mlt". The name of this object will be set to
    "MultiLine". 3 data points are defined "l1", "l2", and "l3", and 3 values are provided. Finally, the object
    is submitted to MLOps by calling mlops.set_stat(mlt)

    """
    @staticmethod
    def verify_multi_line_graph_data(data):
        if not isinstance(data, MultiLineGraph):
            raise MLOpsException("Data to multi line graph should be a MultiLineGraph object")

        return True

    def __init__(self):
        self._name = "MultiLine"
        self._label_list = []
        self._data = []

    def name(self, name):
        """
        Set the name of the MultiLineGraph object

        :param name: Multi-line graph name
        :return: self
        """
        self._name = name
        return self

    def labels(self, label_list):
        """
        Set the labels for this multi-line graph

        :param label_list: List of strings to use as labels
        :return: self
        """
        if not isinstance(label_list, list):
            raise MLOpsException("Labels should be provided as a list")

        if not all(isinstance(item, six.string_types) for item in label_list):
            raise MLOpsException("Labels should be strings")

        self._label_list = copy.deepcopy(label_list)
        return self

    def data(self, vec):
        """
        Set the data point values to use.

        :param vec: List of data points. The number of data points should match the number of labels.
        :return: self
        """
        if not isinstance(vec, list):
            raise MLOpsException("data provided to multi line graph should be a list")
        if not all(isinstance(item, (six.integer_types, float)) for item in vec):
            raise MLOpsException("data provided to multi line graph should be a list of int or floats")
        self._data = copy.deepcopy(vec)
        return self

    def _to_dict(self):
        dd = {}
        for label, value in zip(self._label_list, self._data):
            dd[label] = value
        return dd

    def get_mlops_stat(self, model_id):

        if len(self._data) == 0:
            raise MLOpsException("There is no data in multi line graph")

        if len(self._label_list) == 0:
            raise MLOpsException("MultiLineGraph labels number does not match number of data points")

        if len(self._data) != len(self._label_list):
            raise MLOpsException("number of data point does not match number of labels")

        multiline_data = self._to_dict()

        mlops_stat = MLOpsStat(name=self._name,
                               stat_type=InfoType_pb2.General,
                               graph_type=StatGraphType.MULTILINEGRAPH,
                               mode=StatsMode.TimeSeries,
                               data=multiline_data,
                               model_id=model_id)
        return mlops_stat
