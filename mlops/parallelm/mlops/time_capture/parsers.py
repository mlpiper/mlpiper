import pandas as pd
from datetime import datetime
from parallelm.mlops.mlops_exception import MLOpsException
import datetime
import json
from collections import OrderedDict
from parallelm.mlops.time_capture.json_to_df import JsonToDf
from parallelm.mlops.stats_category import StatGraphType
from parallelm.mlops.time_capture.untar_timeline_capture import UntarTimelineCapture
import shutil


class Parsers(UntarTimelineCapture):
    """This class provides multiple functions that are used for the MCenter time capture
    format converter

    """
    def __init__(self, input_timeline_capture, tmpdir):
        """Initialize the parameters of the parser."""
        self._attribute_names_list = []
        self._df_name = {}
        self._heatmap_df_file = {}
        self._matrix_df_file = {}
        self._multilinegraph_df_file = {}
        self._multigraph_df_file = {}
        self._bar_df_file = {}
        self._kpi_df_file = {}
        self._sys_stat_df_file = {}
        self._aggregate_df_file = {}
        self._events_df_file = {}
        self._file_names = []
        self._attribute_names_list = []
        self._files = {}
        self._mlapp_id = ""
        self._model_policy = ""
        self._nodes_number = 0
        self._nodes_id = []
        self._nodes_type = []
        UntarTimelineCapture.__init__(self, input_timeline_capture, tmpdir)
        self._timeline_capture = {}
        self._extract_timeline_capture()

    def _extract_timeline_capture(self):
        """
        The method extracts bar graphs and events from the time capture files.

        :param self:
        :return:
        """
        try:
            self.untar_timeline_capture()
            extracted_dir = self._extracted_dir
        except Exception as err:
            raise MLOpsException(err)

        for file_name in self._file_names:
            if '-stats-' in file_name:
                self._extract_stats(file_name)
                if file_name in self._bar_df_file.keys():
                    self._name_stats_df(file_name, self._bar_df_file[file_name])
                if file_name in self._multilinegraph_df_file.keys():
                    self._name_stats_df(file_name, self._multilinegraph_df_file[file_name])
                if file_name in self._matrix_df_file.keys():
                    self._name_stats_df(file_name, self._matrix_df_file[file_name])
                if file_name in self._multigraph_df_file.keys():
                    self._name_stats_df(file_name, self._multigraph_df_file[file_name])

            elif 'events' in file_name:
                self._events_df_file = pd.read_csv(extracted_dir + file_name, na_filter=False)

            elif 'aggregate' in file_name:
                self._aggregate_df_file[file_name] = pd.read_csv(extracted_dir + file_name,
                                                                 na_filter=False)

            elif 'sysstat' in file_name:
                self._sys_stat_df_file[file_name] = pd.read_csv(extracted_dir + file_name,
                                                                na_filter=False)

            elif 'ion-details' in file_name:
                print("parse ion-details file")
                self._parse_mlapp(extracted_dir + file_name)

            with open(self._extracted_dir + str(file_name), 'r') as f:
                self._files[file_name] = f.read()
        shutil.rmtree(self._tmpdir)
        return

    def _extract_stats(self, file_name):
        """
        This function extracts the stats data of a stats file, converts them to a table format and
        gathers them in a file.
        each row is spanned to:
        Name, time, keys, values

        :param self:
        :param file_name:
        :return:
        """
        j2df = JsonToDf()
        try:
            # get from the databases row according to their type:
            # bargraph, linegraph, multilinegraph, multigraph and matrix
            # each type is stored in a separate file per variable
            header_list = self._timeline_capture[file_name + 'header']
            graph_type_index = header_list.index('graphType')
            value_index = header_list.index('value')
            time_index = header_list.index('time')
            type_index = header_list.index('mode')  # TODO: Where is the KPI indication?
            name_index = header_list.index('name')

            raw_bar_df = pd.DataFrame([])
            raw_heat_df = pd.DataFrame([])
            raw_multiline_df = pd.DataFrame([])
            raw_multigraph_df = pd.DataFrame([])
            raw_matrix_df = pd.DataFrame([])
            raw_kpi_df = pd.DataFrame([])
            for point in self._timeline_capture[file_name]:  # parse the data
                parsed_entry = {}
                parsed_entry["Name"] = point[name_index]
                parsed_entry["time"] = point[time_index]
                parsed_entry["datetime"] = datetime.datetime. \
                    fromtimestamp(float(point[time_index])/1e9) \
                    .time().strftime("%H:%M:%S")
                if point[value_index] != '':
                    input_entry = json.loads(point[value_index], object_pairs_hook=OrderedDict)
                    gt = point[graph_type_index]
                    st = point[type_index]
                    if gt == StatGraphType.BARGRAPH:
                        parsed_entry["keys"], parsed_entry["values"] = j2df.parse_bar(input_entry)
                        raw_bar_df = raw_bar_df.append(pd.Series(parsed_entry), ignore_index=True)
                    if gt == StatGraphType.HEATMAP:
                        parsed_entry["keys"], parsed_entry["values"] =\
                            j2df.parse_multiline(input_entry)
                        raw_heat_df = raw_heat_df.append(pd.Series(parsed_entry), ignore_index=True)
                    if (gt == StatGraphType.LINEGRAPH) or (gt == StatGraphType.MULTILINEGRAPH) \
                            or (gt == StatGraphType.HEATMAP):
                        parsed_entry["keys"], parsed_entry["values"] =\
                            j2df.parse_multiline(input_entry)
                        raw_multiline_df = raw_multiline_df.append(pd.Series(parsed_entry),
                                                                   ignore_index=True)
                    if st == StatGraphType.KPI:
                        parsed_entry["keys"], parsed_entry["values"] =\
                            j2df.parse_multiline(input_entry)
                        raw_kpi_df = raw_kpi_df.append(pd.Series(parsed_entry),
                                                       ignore_index=True)
                    if gt == StatGraphType.GENERAL_GRAPH:
                        parsed_entry = j2df.parse_multigraph(parsed_entry, input_entry)
                        raw_multigraph_df = raw_multigraph_df.append(pd.Series(parsed_entry),
                                                                     ignore_index=True)
                    if gt == StatGraphType.MATRIX:
                        matrix_line_df = j2df.parse_matrix(parsed_entry, input_entry)
                        raw_matrix_df = raw_matrix_df.append(matrix_line_df, ignore_index=True)
            if not raw_bar_df.empty:
                self._bar_df_file[file_name] = raw_bar_df
            if not raw_multiline_df.empty:
                self._multilinegraph_df_file[file_name] = raw_multiline_df
            if not raw_heat_df.empty:
                self._heatmap_df_file[file_name] = raw_heat_df
            if not raw_multigraph_df.empty:
                self._multigraph_df_file[file_name] = raw_multigraph_df
            if not raw_matrix_df.empty:
                self._matrix_df_file[file_name] = raw_matrix_df
            if not raw_kpi_df.empty:
                self._kpi_df_file[file_name] = raw_kpi_df
        except Exception as err:
            raise MLOpsException(err)

    def _parse_mlapp(self, file_path):
        """
        This function extracts the mlapp-details.txt file.

        :param self
        :param file_path: File Path
        :return:
        """
        with open(file_path, 'r') as f:
            reader1 = f.read()
        reader1 = reader1.replace('wfNodes', ',wfNodes')
        reader1 = reader1.replace('GroupID', ',GroupID')
        reader1 = reader1.replace('Mode', ',Mode')
        reader1 = reader1.replace('Type', ',Type')
        reader1 = reader1.replace(',', '","')
        reader1 = reader1.replace(':', '":"')
        reader1 = reader1.replace('" [', '[{"')
        reader1 = reader1.replace(']"', '"}]')
        reader1 = reader1.replace(' ', '')
        reader1 = reader1.replace(',"PipelineID', '},{"PipelineID')
        nd1 = '{"' + reader1 + '"}'
        kd = json.loads(nd1)
        self._mlapp_id = kd["WorkflowID"]
        self._model_policy = kd["modelPolicy"]
        self._nodes_number = len(kd["wfNodes"])
        self._nodes_id = [nodes["PipelineID"] for nodes in kd["wfNodes"]]
        self._nodes_type = [nodes["Type"] for nodes in kd["wfNodes"]]

    def _name_stats_df(self, filename, df):
        """
        Create a dataframe per attribute name (ex: for overlapping training/inference
        plot of histogram)

        :param self:
        :param df: Dataframe to process
        :param filename: File name
        :return:
        """
        try:
            name_list = df.Name.unique()
            local_df_name = {}
            # separate according to names
            for name in name_list:
                if name not in self._attribute_names_list:
                    self._attribute_names_list.append(name)
                    self._df_name[name] = pd.DataFrame()
                local_df_name[name] = pd.DataFrame()
                local_df_name[name] = local_df_name[name].append(df[df['Name'] == name],
                                                                 ignore_index=True)
                local_df_name[name]['FileName'] = filename
                self._df_name[name] = self._df_name[name].append(local_df_name[name],
                                                                 ignore_index=True)
                self._df_name[name] = self._df_name[name].sort_values(by=["time"])  # Time sort
                self._df_name[name] = self._df_name[name].reset_index()  # reset the lines index
                self._df_name[name] = self._df_name[name].drop('index', axis=1)
        except Exception as err:
            raise MLOpsException(err)
