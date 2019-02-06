
from parallelm.mlops.stats.mlops_stat import MLOpsStat
from parallelm.mlops.stats_category import StatGraphType, StatsMode

from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats.mlops_stat_getter import MLOpsStatGetter
from parallelm.mlops.stats.stats_utils import check_vec_of_numbers
from parallelm.mlops import Constants
from parallelm.protobuf import InfoType_pb2

from collections import OrderedDict
from functools import reduce
import numpy
import sys
import copy
import six


class Graph(MLOpsStatGetter):
    """
    The Graph object allows users to specify a generic function by providing the x and y values.
    Unlike line and multi-line statistics, the graph is calculated and replaced for every pipeline run.

    :Example:

    >>> x_series = [1, 2, 3, 4, 5, 6]
    >>> y_series = [11, 12, 13, 14, 15, 16]
    >>> gg = Graph().name("gg").set_x_series(x_series).add_y_series(label="y1", data=y_series)
    >>> gg.x_title("x-axis-title")
    >>> gg.y_title("y-axis-title")
    >>> mlops.set_stat(gg)

    The above example creates the x_series and y_series as vectors of integers.
    Then a Graph object is created, named "gg", its x-axis data is set to x_series, and its y-axis
    data is set to y_series with label "y1".
    The final line reports the Graph object to MLOps.
    """

    def __init__(self):
        self._name = "Graph"

        self._x_title = "X - Title"
        self._y_title = "Y - Title"

        self._x_series = None
        self._y_series = []
        self._x_annotations = []
        self._y_annotations = []
        self._x_axis_type = Constants.STAT_CONTINUOUS

    def name(self, name):
        """
        Set the name of the graph

        :param name: graph name
        :return: self
        """
        self._name = name
        return self

    def x_title(self, title):
        """
        Set the title for the x-axis

        :param title: title for the x-axis
        :return: self
        """
        self._x_title = title
        return self

    def y_title(self, title):
        """
        Set the title for the y-axis

        :param title: title for the y-axis
        :return: self
        """
        self._y_title = title
        return self

    def set_categorical(self):
        """
        Set the x-axis type for categorical values

        :return: self
        """
        self._x_axis_type = Constants.STAT_CATEGORICAL
        return self

    def set_continuous(self):
        """
        Set the y-axis type for continuous values

        :return: self
        """
        self._x_axis_type = Constants.STAT_CONTINUOUS
        return self

    def set_x_series(self, x_series):
        """
        Set the data for the x axis

        :param x_series: vector of values on the x axis
        :return: self
        """
        if self._x_series is None:
            check_vec_of_numbers(x_series, error_prefix="x_series")
            self._x_series = copy.deepcopy(x_series)
        else:
            raise MLOpsException("Graph x_series is already set")
        return self

    def add_y_series(self, label, data):
        """
        Set the data for the y axis

        :param label: label for the y data
        :param data: vector of values on the y axis
        :return: self
        :raises: MLOpsException
        """
        if self._x_series is None:
            raise MLOpsException("Can not add a y_series before setting x_series data")
        check_vec_of_numbers(data, error_prefix="y_series data")
        if len(data) != len(self._x_series):
            raise MLOpsException("y_series data is not in the same lenght of x_series data")

        self._y_series.append({"label": label, "data": data})
        return self

    def annotate(self, label, x=None, y=None):
        """
        Annotate a point in the graph

        :param label: annotation
        :param x: x value to annotate
        :param y: y value to annotate
        :return: self
        :raises: MLOpsException
        """
        if self._x_series is None:
            raise MLOpsException("Can not add annotations before setting x_series data")

        if x is not None and y is not None:
            raise MLOpsException("Annotation can be either x or y annotation")

        if x is not None:
            if not isinstance(x, (six.integer_types, float)):
                raise MLOpsException("x argument should be a number")
            self._x_annotations.append({"label": label, "value": x})
        elif y is not None:
            if not isinstance(y, (six.integer_types, float)):
                raise MLOpsException("y argument should be a number")
            self._y_annotations.append({"label": label, "value": y})
        else:
            raise MLOpsException("Annotation must provide an axis")

        return self

    def _to_dict(self):

        graph_dict = OrderedDict()
        graph_dict["x_title"] = self._x_title
        graph_dict["y_title"] = self._y_title
        graph_dict["x_axis_type"] = self._x_axis_type
        graph_dict["x_axis_tick_postfix"] = ""
        graph_dict["x_series"] = self._x_series
        graph_dict["y_series"] = self._y_series
        graph_dict["x_annotation"] = self._x_annotations
        graph_dict["y_annotation"] = self._y_annotations

        return graph_dict

    def get_mlops_stat(self, model_id):

        if self._x_series is None:
            raise MLOpsException("No x_series was provided")
        if len(self._y_series) == 0:
            raise MLOpsException("At leat one y_series should be provided")

        tbl_data = self._to_dict()

        mlops_stat = MLOpsStat(name=self._name,
                               stat_type=InfoType_pb2.General,
                               graph_type=StatGraphType.GENERAL_GRAPH,
                               mode=StatsMode.Instant,
                               data=tbl_data,
                               model_id=model_id)
        return mlops_stat

class MultiGraph(MLOpsStatGetter):
    """
    A MultiGraph is similar to a Graph object, but it supports more than one line.

    :Example:

    >>> x1_series = [0, 2, 4, 6]
    >>> y1_series = [11, 12, 13, 14]
    >>> x2_series = [1, 3, 5, 7]
    >>> y2_series = [15, 16, 17, 18]
    >>> gg = MultiGraph().name("gg")
    >>> gg.add_series(x=x1_series,label="y1", y=y1_series)
    >>> gg.add_series(x=x2_series,label="y2", y=y2_series)
    >>> mlops.set_stat(gg)

    In this example, we create 2 sets of x and y data.
    The we create a MultiGraph object named "gg".
    In the next line, we set the x and y data for the first line and label it "y1".
    In the next line, we set the x and y data for the second line and label it "y2".
    In the final line, the MultiGraph is exported to MLops.
    """

    def __init__(self):
        self._name = "MultiGraph"

        self._x_title = "X - Title"
        self._y_title = "Y - Title"

        self._x_series = []
        self._y_series = []
        self._labels = []

        self._x_annotations = []
        self._y_annotations = []
        self._x_axis_type = Constants.STAT_CONTINUOUS

    def name(self, name):
        """
        Set the name of the chart

        :param name: multi-graph name
        """
        self._name = name
        return self

    def x_title(self, title):
        """
        Set the title for the x axis

        :param title: title for the x axis
        :return: self
        """
        self._x_title = title
        return self

    def y_title(self, title):
        """
        Set the title for the y axis

        :param title: title for the y axis
        :return: self
        """
        self._y_title = title
        return self

    def set_categorical(self):
        """
        Set the data type for the x axis as categorical.
        Setting to categorical aids in choosing the correct type of visualization.

        :return: self
        """
        self._x_axis_type = Constants.STAT_CATEGORICAL
        return self

    def set_continuous(self):
        """
        Set the data type for the x axis as continuous.
        Setting the data type aids in creating the correct type of visualization.

        :return: self
        """
        self._x_axis_type = Constants.STAT_CONTINUOUS
        return self

    def add_series(self, label, x, y):
        """
        Add a new line to the MultiGraph object

        :param label: name for the line
        :param x: vector of values for the x axis
        :param y: vector of values for the y axis
        :return: self
        """
        check_vec_of_numbers(y, error_prefix="y_series data")
        if len(y) != len(x):
            raise MLOpsException("y_series data is not in the same lenght of x_series data")

        self._labels.append(label)
        self._x_series.append(copy.deepcopy(x))
        self._y_series.append(copy.deepcopy(y))

        return self

    def annotate(self, label, x=None, y=None):
        """
        Add an annotation to a point on a line

        :param label: annotation to add
        :param x: x value for the annotation
        :param y: y value for the annotation
        :return: self
        """
        if self._x_series is None:
            raise MLOpsException("Can not add annotations before setting x_series data")

        if x is not None and y is not None:
            raise MLOpsException("Annotation can be either x or y annotation")

        if x is not None:
            if not isinstance(x, (six.integer_types, float)):
                raise MLOpsException("x argument should be a number")
            self._x_annotations.append({"label": label, "value": x})
        elif y is not None:
            if not isinstance(y, (six.integer_types, float)):
                raise MLOpsException("y argument should be a number")
            self._y_annotations.append({"label": label, "value": y})
        else:
            raise MLOpsException("Annotation must provide an axis")

        return self

    def _to_dict(self):

        merge_x = sorted(list(reduce(lambda a,b: set(a)|set(b), self._x_series, [])))
        merge_y = [[self._y_series[i][self._x_series[i].index(a)] if a in self._x_series[i] else None
                    for a in merge_x] for i in range(len(self._x_series))]

        y_and_label = [{"label": self._labels[i], "data": merge_y[i]} for i in range(len(merge_y))]

        if (self._x_axis_type == Constants.STAT_CATEGORICAL):
            merge_x = [str(x) for x in merge_x]
        if (self._x_axis_type == Constants.STAT_CONTINUOUS):
            merge_x = [numpy.sign(y) * sys.float_info.max if abs(y) == float('inf') else y for y in merge_x]

        graph_dict = OrderedDict()
        graph_dict["x_title"] = self._x_title
        graph_dict["y_title"] = self._y_title
        graph_dict["x_axis_type"] = self._x_axis_type
        graph_dict["x_axis_tick_postfix"] = ""
        graph_dict["x_series"] = merge_x
        graph_dict["y_series"] = y_and_label
        graph_dict["x_annotation"] = self._x_annotations
        graph_dict["y_annotation"] = self._y_annotations

        return graph_dict

    def get_mlops_stat(self, model_id):

        if self._x_series is None:
            raise MLOpsException("No x_series was provided")
        if len(self._y_series) == 0:
            raise MLOpsException("At leat one y_series should be provided")

        tbl_data = self._to_dict()

        mlops_stat = MLOpsStat(name=self._name,
                               stat_type=InfoType_pb2.General,
                               graph_type=StatGraphType.GENERAL_GRAPH,
                               mode=StatsMode.Instant,
                               data=tbl_data,
                               model_id=model_id)
        return mlops_stat

