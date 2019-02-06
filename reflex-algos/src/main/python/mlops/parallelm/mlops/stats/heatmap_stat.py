from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats.mlops_stat import MLOpsStat
from parallelm.mlops.stats.mlops_stat_getter import MLOpsStatGetter
from parallelm.mlops.stats.stats_utils import check_list_of_str, check_vec_of_numbers
from parallelm.mlops.stats_category import StatGraphType, StatsMode
from parallelm.protobuf import InfoType_pb2
import copy


class _HeatMapStat(MLOpsStatGetter):
    """
    A container for heatmap graph data that allows users to specify a generic heatmap graph by providing the feature and its values.
    The Heatmap graph is calculated and replaced for every pipeline run.

    :Example:
    >>> from parallelm.mlops import mlops
    >>> hm = _HeatMapStat().name("hm").features(["g1", "g2"]).data([1.1, 2.4])
    >>> mlops.set_stat(hm)

    This example creates and populates a BarGraph structure, then reports it to MLOps.
    """


    def __init__(self):
        self._name = "HeatMap"
        self._feature_names = []
        self._data = []

    def name(self, name):
        """
        Set the name of the heatmap. This will used in the MLOps UI.

        :param name: heatmap name
        :return: self (the heatmap object)
        """
        self._name = name
        return self

    def features(self, features_names):
        """
        Set the feature names

        :param features_names: List of names
        :return: self
        """
        check_list_of_str(features_names, error_prefix="feature names")

        self._feature_names = copy.deepcopy(features_names)
        return self

    def data(self, vec):
        """
        Set the data

        :param vec: list of data values
        :return: self
        """
        check_vec_of_numbers(vec, "values for heat graph")
        self._data = copy.deepcopy(vec)
        return self

    def _to_dict(self):
        dd = {}
        for label, value in zip(self._feature_names, self._data):
            dd[label] = value

        return dd

    def get_mlops_stat(self, model_id):

        if len(self._data) == 0:
            raise MLOpsException("There is no data in heat graph")

        if len(self._feature_names) == 0:
            raise MLOpsException("No columns names were provided")

        if len(self._data) != len(self._feature_names):
            raise MLOpsException("Number of data point does not match number of columns")

        data = self._to_dict()

        mlops_stat = MLOpsStat(name=self._name,
                               stat_table=self._name,
                               stat_type=InfoType_pb2.General,
                               graph_type=StatGraphType.HEATMAP,
                               mode=StatsMode.TimeSeries,
                               data=data,
                               model_id=model_id)
        return mlops_stat
