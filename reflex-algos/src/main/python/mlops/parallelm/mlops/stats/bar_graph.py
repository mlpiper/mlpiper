from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats.mlops_stat import MLOpsStat
from parallelm.mlops.stats.mlops_stat_getter import MLOpsStatGetter
from parallelm.mlops.stats.stats_utils import check_list_of_str, check_vec_of_numbers
from parallelm.mlops.stats_category import StatGraphType, StatsMode
from parallelm.protobuf import InfoType_pb2
import copy


class BarGraph(MLOpsStatGetter):
    """
    A container for bar graph data that allows users to specify a generic bar graph by providing the bar's x and y
    values. The Bar graph is calculated and replaced for every pipeline run.

    :Example:

    >>> mlt = BarGraph().name("bar").cols(["g1", "g2"]).data([1.1, 2.4])
    >>> mlops.set_stat(mlt)

    This example creates and populates a BarGraph structure, then reports it to MLOps.
    """

    def __init__(self):
        self._name = "BarGraph"
        self._col_names = []
        self._data = []
        self._is_data_continuous = False

    def name(self, name):
        """
        Set the name of the bargraph. This will used in the MLOps UI.

        :param name: bargraph name
        :return: self (the BarGraph object)
        """
        self._name = name
        return self

    def columns_names(self, col_names):
        """
        Set the column names

        :param name_list: List of names
        :return: self
        """
        self._col_names = copy.deepcopy(col_names)
        return self

    def cols(self, col_names):
        """
        Set the column names

        :param col_names: List of names
        :return: self
        """
        return self.columns_names(col_names)

    def data(self, vec):
        """
        Set the data

        :param vec: list of data values
        :return: self
        """
        check_vec_of_numbers(vec, "Values for bar graph")
        self._data = copy.deepcopy(vec)
        return self

    def as_continuous(self):
        """
        While displaying data into UI, it will create format with bin entries separated by word -to-.
        Since, continuous histogram is continuous values and it is better to represent as range instead of single value.

        :return: current object
        """
        self._is_data_continuous = True
        return self

    def _to_dict(self):
        if len(self._data) == 0:
            raise MLOpsException("There is no data in bar graph")

        if len(self._col_names) == 0:
            raise MLOpsException("No columns names were provided")

        dd = {}
        ll = []

        edge_values = []
        bin_values = self._data

        if self._is_data_continuous:
            # TODO: Combine all continuous data representation generation into single place so that it would be easy to change in future.

            for each_edge_index in range(0, len(self._col_names) - 1):
                edge_values.append(
                    str(self._col_names[each_edge_index]) +
                    " to " +
                    str(self._col_names[each_edge_index + 1]))
        else:
            edge_values = self._col_names

        check_list_of_str(edge_values, error_prefix="Columns names")

        if len(edge_values) != len(bin_values):
            raise MLOpsException(
                "Number of data point does not match number of columns. "
                "edges values are: {} and bin values are {}".format(edge_values, bin_values))

        for label, value in zip(edge_values, bin_values):
            ll.append({label: value})

        dd[self._name] = ll
        return dd

    def get_mlops_stat(self, model_id):

        data = self._to_dict()

        mlops_stat = MLOpsStat(name=self._name,
                               stat_type=InfoType_pb2.General,
                               graph_type=StatGraphType.BARGRAPH,
                               mode=StatsMode.Instant,
                               data=data,
                               model_id=model_id)
        return mlops_stat
