import pandas as pd
import ast
import json
import pickle
import binascii
from collections import OrderedDict


from parallelm.mlops.constants import Constants, HistogramType, MatrixType
from parallelm.mlops.stats_category import StatGraphType
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.constants import DataframeColNames


expected_jsons = [{"id":"","configurable":False,"mlStat":{"name":"","graphType":"BARGRAPH"},"values":[{"name":"stat","columns":["time","value"],"values":[["2018-02-08T18:27:28.273Z","{\"bar\": \"{\\\"Col1\\\": 10,\\\"Col2\\\": 15}\"}"]]}]},
                  {"id":"","configurable":False,"mlStat":{"name":"","id":"","graphType":"MULTILINEGRAPH"},"values":[{"name":"stat","columns":["time","value"],"values":[["2018-02-08T18:27:28.262Z","{\"l2\": 15, \"l3\": 22, \"l1\": 0}"]]}]},
                  {"id":"bebd75ee-0ce3-4ea8-90fa-ad90316b71b3","configurable":False,"mlStat":{"name":"","id":"", "graphType":"LINEGRAPH"},"values":[{"name": "stat","columns":["time","value"],"values":[["2018-02-08T18:27:27.183Z","{\"0\":1.0}"],["2018-02-08T18:27:27.187Z","{\"0\":2.0}"]]}]}]


class DataFrameHelper(object):
    """
    Helper class to convert json to dataframes.
    """

    @staticmethod
    def _update_data_dict(data, key_values, time_stamp, column_name):
        """
        Function to append dictionary to existing dictionary of columns : values
        :return: Updated dictionary
        """
        for k, v in key_values.items():
            if k in data:
                data[k].append(v)
            else:
                data[k] = [v]

        if column_name in data:
            data[column_name].append(time_stamp)
        else:
            data[column_name] = [time_stamp]

        return data

    @staticmethod
    def _create_dict(graph_type, values, stat_name, columns, no_of_lines):
        """
        Function to create dictionary with given number of columns : values
        :return: Dictionary
        :raises: MLOpsException
        """
        data_dictionary = {}
        data = {}

        lines = 0

        for value in values:
            if lines == no_of_lines:
                data_dictionary.update(data)
                return data_dictionary
            if len(value) == len(columns) and len(value) >= Constants.MIN_STAT_COLUMNS:
                if graph_type == StatGraphType.LINEGRAPH:
                    if columns[1] in data:
                        data[columns[1]].append(list(ast.literal_eval(value[1]).values())[0])
                        data[columns[0]].append(value[0])
                    else:
                        data[columns[1]] = list(ast.literal_eval(value[1]).values())
                        data[columns[0]] = [value[0]]

                elif graph_type == StatGraphType.MULTILINEGRAPH:
                    temp_values = ast.literal_eval(value[1])
                    data.update(DataFrameHelper._update_data_dict(data, temp_values, value[0], columns[0]))

                elif graph_type == StatGraphType.BARGRAPH:
                    temp_values = ast.literal_eval(value[1])
                    for k, v in temp_values.items():  # To ignore legend of graph key, need to go one more level down
                        temp_bar_values = {}
                        if type(v) == list:
                            hist = []
                            bin_edges = []
                            keys = []
                            for item in v:
                                hist.append(list(item.values())[0])
                                key = list(item.keys())[0]
                                keys.append(key)
                                for bin_edge in key.split(HistogramType.HISTOGRAM_BIN_IDENTIFIER):
                                    if bin_edge not in bin_edges:
                                        bin_edges.append(bin_edge)

                            if all(HistogramType.HISTOGRAM_BIN_IDENTIFIER in edge for edge in keys):
                                temp_bar_values[HistogramType.HISTOGRAM_TYPE_COLUMN] = \
                                    StatGraphType.HISTOGRAM_TYPE_CONTIGOUOUS
                            else:
                                temp_bar_values[HistogramType.HISTOGRAM_TYPE_COLUMN] = \
                                    StatGraphType.HISTOGRAM_TYPE_CATEGORICAL

                            temp_bar_values[HistogramType.HISTOGRAM_COLUMN] = hist
                            temp_bar_values[HistogramType.BIN_EDGES_COLUMN] = bin_edges

                            temp_bar_values[HistogramType.STAT_NAME_COLUMN] = stat_name
                        else:
                            temp_bar_values = ast.literal_eval(v)

                        data.update(DataFrameHelper._update_data_dict(data, temp_bar_values, value[0], columns[0]))
                elif graph_type == StatGraphType.OPAQUE:
                    vv = ast.literal_eval(value[1])
                    key, opq_data = vv.popitem()
                    opq_data = pickle.loads(binascii.unhexlify(opq_data.encode('utf-8')))
                    if len(data) == 0:
                        data[columns[0]] = [value[0]]
                        data[DataframeColNames.OPAQUE_DATA] = [opq_data]
                elif graph_type == StatGraphType.MATRIX:
                    temp_values = json.loads(value[1], object_pairs_hook=OrderedDict)
                    for k in temp_values.keys():
                        temp_bar_values = {}
                        temp_bar_values[MatrixType.ROW_NAME] = k
                        temp_bar_values[MatrixType.VALUES] = list(temp_values[k].values())
                        temp_bar_values[MatrixType.COLUMNS] = list(temp_values[k].keys())
                        data.update(DataFrameHelper._update_data_dict(data, temp_bar_values, value[0], columns[0]))
                elif graph_type == StatGraphType.GENERAL_GRAPH:
                    temp_values = ast.literal_eval(value[1])
                    data.update(DataFrameHelper._update_data_dict(data, temp_values, value[0], columns[0]))
                else:
                        data[columns[0]].append(value[0])
                        data[DataframeColNames.OPAQUE_DATA].append(opq_data)

            else:
                raise MLOpsException("Invalid json, Number of columns and values don't match. Given columns: {}, "
                                       "values: {}. \nExpected json formats:{}".format(columns, value, expected_jsons))
            lines += 1

        data_dictionary.update(data)

        return data_dictionary

    @staticmethod
    def single_histogram_dict(json_dict, stat_name):
        hist_values = []
        bin_edges = []

        all_bin_str = []
        for bin_and_value in json_dict:

            bin_str = None
            value = None
            for item in bin_and_value.items():
                bin_str = item[0]
                value = item[1]
                break

            hist_values.append(value)
            all_bin_str.append(bin_str)
            for bin_edge in bin_str.split(HistogramType.HISTOGRAM_BIN_IDENTIFIER):
                if bin_edge not in bin_edges:
                    bin_edges.append(bin_edge)

        if all(HistogramType.HISTOGRAM_BIN_IDENTIFIER in edge for edge in all_bin_str):
            hist_type = StatGraphType.HISTOGRAM_TYPE_CONTIGOUOUS
        else:
            hist_type = StatGraphType.HISTOGRAM_TYPE_CATEGORICAL

        hist_dict = {
            HistogramType.STAT_NAME_COLUMN: stat_name,
            HistogramType.HISTOGRAM_TYPE_COLUMN: hist_type,
            HistogramType.HISTOGRAM_COLUMN: hist_values,
            HistogramType.BIN_EDGES_COLUMN: bin_edges
        }
        return hist_dict

    @staticmethod
    def multi_histogram_df(multi_histogram_dict):
        hist_list = []
        for attribute in multi_histogram_dict:
            hist_dict = DataFrameHelper.single_histogram_dict(multi_histogram_dict[attribute], attribute)
            hist_list.append(hist_dict)
        df = pd.DataFrame(data=hist_list)
        return df

    @staticmethod
    def create_data_frame(json, no_of_lines=-1):
        """
        Function to parse json to create data frame
        :param json:        Input json
        :param no_of_lines: Number of lines to parse. This will be the number of rows in data frame.
        :return:            Data Frame
        :raises MLOpsException for invalid json or json with an unsupported graph type.
        """
        graph_type = ""

        # Return empty dataframe in case of empty response
        if len(json) == 0:
            return pd.DataFrame(data={})

        # The json user passes cannot be trusted to be ordered dictionary, so we traverse twice to get graphType first.
        if Constants.STAT_GRAPHTYPE in json:
            graph_type = json[Constants.STAT_GRAPHTYPE]
        else:
            raise MLOpsException("Invalid json, {} not found in given json: {} \n Expected json formats: {}".format(
                Constants.STAT_GRAPHTYPE, json, expected_jsons))

        if graph_type not in StatGraphType.MLOPS_STAT_GRAPH_TYPES:
            raise MLOpsException("Unsupported graph type: '{}'. Supported graph types: {}".format(
                graph_type, StatGraphType.MLOPS_STAT_GRAPH_TYPES))

        # Pass values dictionary to get data frame
        if len(json[Constants.STAT_VALUES]) > 0:
            columns = json[Constants.STAT_VALUES][0][Constants.STAT_COLUMNS]

            if no_of_lines == -1:
                no_of_lines = len(json[Constants.STAT_VALUES][0][Constants.STAT_VALUES])

            stat_name = json[Constants.STAT_VALUES][0][Constants.STAT_NAME]

            data_frame = pd.DataFrame(data=DataFrameHelper._create_dict(
                    graph_type, json[Constants.STAT_VALUES][0][Constants.STAT_VALUES], stat_name, columns, no_of_lines))
            return data_frame

        # If format is unexpected return empty data frame
        return pd.DataFrame(data={})

    @staticmethod
    def add_col_with_same_value(df, col_name, value):
        nr_rows = len(df)
        value_list = [value] * nr_rows
        df[col_name] = value_list
