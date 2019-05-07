import numpy as np

from parallelm.mlops.mlops_exception import MLOpsStatisticsException
from parallelm.mlops.stats.graph import Graph
from parallelm.mlops.stats.multi_line_graph import MultiLineGraph
from parallelm.mlops.stats.single_value import SingleValue
from parallelm.mlops.stats.table import Table
from parallelm.mlops.stats_category import StatCategory


class MLStatObjectCreator(object):
    @staticmethod
    def get_single_value_stat_object(name, single_value):
        """
        Create Single Value stat object from numerical value
        :param name: Name of stat
        :param single_value: single numeric value
        :return: MLOps Single Value object, time series category
        """
        if isinstance(single_value, (int, float)):
            category = StatCategory.TIME_SERIES
            single_value = \
                SingleValue() \
                    .name(name) \
                    .value(single_value) \
                    .mode(category)

            return single_value, category
        else:
            raise MLOpsStatisticsException \
                ("For outputting {}, {} should be of type numeric but got {}."
                 .format(name, single_value, type(single_value)))

    @staticmethod
    def get_table_value_stat_object(name, list_2d, match_header_pattern=None):
        """
        Create Table Value stat object from list of list. Where first element of 2d list is header. And from remaining lists, list's first index is Row's header.
        :param name: Name of stat
        :param list_2d: 2d representation of table to output
        :param match_header_pattern: If not none, then header of table should match the pattern provided
        :return: MLOps Table Value object, general stat category
        """
        category = StatCategory.GENERAL
        try:
            header = list(map(lambda x: str(x).strip(), list_2d[0]))

            if match_header_pattern is not None:
                assert header == match_header_pattern, \
                    "headers {} is not matching expected headers pattern {}" \
                        .format(header, match_header_pattern)

            len_of_header = len(header)
            table_object = Table().name(name).cols(header)

            for index in range(1, len(list_2d)):
                assert len(list_2d[index]) - 1 == len_of_header, \
                    "length of row value does not match with headers length"

                row_title = str(list_2d[index][0]).strip()
                row_value = list(map(lambda x: str(x).strip(), list_2d[index][1:]))
                table_object.add_row(row_title, row_value)

            return table_object, category
        except Exception as e:
            raise MLOpsStatisticsException \
                ("error happened while outputting table object from list_2d: {}. error: {}".format(list_2d, e))

    @staticmethod
    def get_graph_value_stat_object(name, x_data, y_data, x_title, y_title, legend):
        """
        Create graph object from given data.
        :param name: Name of stat
        :param x_data: X axis data. It has to be numeric list.
        :param y_data: Y axis data. It has to be numeric list.
        :param x_title: X axis title
        :param y_title: Y axis title
        :param legend: Legend of Y axis
        :return: MLOps Graph Value object, general stat category
        """
        category = StatCategory.GENERAL

        if legend is None:
            legend = "{} vs {}".format(y_title, x_title)

        try:
            graph_object = Graph() \
                .name(name) \
                .set_x_series(list(x_data)) \
                .add_y_series(label=legend, data=list(y_data))

            graph_object.x_title(x_title)
            graph_object.y_title(y_title)

            return graph_object, category
        except Exception as e:
            raise MLOpsStatisticsException \
                ("error happened while outputting graph object. error: {}".format(e))

    @staticmethod
    def get_multiline_stat_object(name, list_value, labels=None):
        """
        Create multiline object from list of values. It outputs mulitline from values and legends is index of the values - i.e. 0, 1, ..
        :param name: Name of stat
        :param list_value: list of values to embed in multiline value.
        :return: MLOps Multiline Value object, timeseries stat category
        """
        if isinstance(list_value, list) or isinstance(list_value, np.ndarray):
            category = StatCategory.TIME_SERIES

            # if labels are not provided then it will be 0, 1, .. length of list - 1
            if labels is None:
                labels = range(len(list_value))
            labels = list(map(lambda x: str(x).strip(), labels))

            if (len(labels) == len(list_value)):
                multiline_object = MultiLineGraph() \
                    .name(name) \
                    .labels(labels)

                multiline_object.data(list(list_value))

                return multiline_object, category
            else:
                raise MLOpsStatisticsException(
                    "size of labels associated with list of values to get does not match. {}!={}"
                    .format(len(labels), len(list_value)))
        else:
            raise MLOpsStatisticsException(
                "list_value has to be of type list or nd array but got {}".format(type(list_value)))
