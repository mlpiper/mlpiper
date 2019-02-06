from parallelm.mlops import mlops
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats.mlops_stat import MLOpsStat
from parallelm.mlops.stats.mlops_stat_getter import MLOpsStatGetter
from parallelm.mlops.stats.stats_utils import check_list_of_str
from parallelm.mlops.stats_category import StatGraphType, StatsMode
from parallelm.protobuf import InfoType_pb2


class _HistogramStat(MLOpsStatGetter):
    """
    A container for Histogram graph data that allows users to specify a generic histogram graph by providing the feature and its values.
    The Heatmap graph is calculated and replaced for every pipeline run.

    :Example:

    >>> hist = _HistogramStat().name("hm").add_feature_data(feature = "0", edge = ["1.0 to 3.0", "3.0 to 5.0", "5.0 to 7.0"], bin = [0.5, 0.3, 0.2])

    >>> mlops.set_stat(hist)

    This example creates and populates a BarGraph structure, then reports it to MLOps.
    """

    def __init__(self):
        self._name = "Histogram"
        self._feature_value = {}
        self._stat_type = InfoType_pb2.Health

    def name(self, name):
        """
        Set the name of the Histogram. This will used in the MLOps UI.

        :param name: Histogram name
        :return: self (the Histogram object)
        """
        self._name = name
        return self

    def add_feature_data(self, feature, edge, bin):
        """
        Set the feature names

        :param feature: Name of feature
        :param bin: List of bins which will be x axis
        :param edge: List of edges which will be y axis
        :return: self
        """
        check_list_of_str(edge, error_prefix="edge names")

        ll = []
        for label, value in zip(edge, bin):
            ll.append({label: value})

        self._feature_value[feature] = ll

        return self

    def _to_dict(self):

        return self._feature_value

    def get_mlops_stat(self, model_id):

        if len(self._feature_value) == 0:
            raise MLOpsException("There is no data in histogram graph")

        data = self._to_dict()

        mlops_stat = MLOpsStat(name=self._name,
                               stat_table=self._name,
                               stat_type=self._stat_type,
                               graph_type=StatGraphType.BARGRAPH,
                               mode=StatsMode.Instant,
                               data=data,
                               model_id=model_id)
        return mlops_stat
