import pandas as pd
import numpy as np
en_plt = False
en_tabulate = False
try:
    import matplotlib.pyplot as plt
    en_plt = True
except Exception as err:
    print("not able to load matplotlib.pyplot")
    pass
try:
    from tabulate import tabulate
    en_tabulate = True
except Exception as err:
    print("not able to load tabulate")
    pass


class PlotFunctions:
    """This class provides plot functions for MCenter time capture
    """

    def __init__(self, mtc):
        """Initialized the parameters of the untar class."""
        self._mtc = mtc
        self._events_df_file = mtc.get_events()
        self._file_names = mtc.get_file_names()
        self._matrix_df_file = mtc.get_matrix_df_file()
        self._multigraph_df_file = mtc.get_multigraph_df_file()
        self._attribute_names_list = mtc.get_attribute_names_list()

    def line_plot(self, name, df_name=None):
        """
        Plotting of line graphs per bin per file name

        :param self:
        :param name: attribute name
        :param df_name: attribute df optional
        :return:
        """

        df = df_name
        if df_name is None:
            df = self._mtc.get_stats(name, mlapp_node=None, agent=None, start_time=None, end_time=None)
        node_info = self._mtc.get_nodes()
        if ("keys" in df.columns) and en_plt:
            color_list = ['r', 'b', 'g', 'c', 'k', 'y', '0.75', 'm', '0.25']
            bins, bins_vert = self.hist_bin_adapt(df)
            name_pipeline, td_matrix = self.align_bins(df)
            all_pipelines = list(set(name_pipeline))
            num_of_bins = len(bins)
            for pipelines_elements in all_pipelines:
                fig = plt.figure()
                ax3 = fig.add_subplot(111)
                # Get Elements for the specific pipeline
                file_index = [i for i, e in enumerate(name_pipeline)
                              if e == pipelines_elements]
                lineplot_time1 = df["time"].iloc[file_index].tolist()
                time_values1 = df["datetime"].iloc[file_index].tolist()

                # Plot per bin
                for bin_index in range(0, num_of_bins):
                    td_matrix1 = td_matrix[file_index, bin_index]
                    ax3.plot(lineplot_time1, td_matrix1,
                             color=color_list[bin_index % (len(color_list))],
                             label="Bin " + str(bins[bin_index]), linewidth=4)
                ax3.set_xticklabels(time_values1, ha='center')
                ax3.tick_params(labelsize=8)
                self.annotate_events(figure=ax3)
                ax3.legend(bbox_to_anchor=(1, 1), prop={'size': 10}, loc=2)
                ax3.grid()
                pipeline_index = [i for i, node_elements in enumerate(node_info)
                 if node_elements[0] == pipelines_elements]
                ax3.set_title('Linegraph vs time for ' + str(name) + " @ Pipeline: "
                              + node_info[pipeline_index[0]][1] + ", " + node_info[pipeline_index[0]][2])
                ax3.set_ylabel('value')
                ax3.set_xlabel('time')

    def annotate_events(self, figure):
        """
        Event annotation in the plot

        :param self:
        :param figure: Figure
        :return:
        """
        # set marker in model change event (blue) and in alerts (red)
        first_model = True
        first_alert = True
        for location_index in range(0, self._events_df_file.shape[0]):
            if self._events_df_file["eventType"].loc[location_index] == "Model":
                if first_model:
                    figure.axvline(x=self._events_df_file["time"]
                                   .loc[location_index], linewidth=2, linestyle='dashed',
                                   color='b', label='model_Update')
                    first_model = False
                else:
                    figure.axvline(x=self._events_df_file["time"]
                                   .loc[location_index], linewidth=2, linestyle='dashed', color='b')
            if self._events_df_file["raiseAlert"].loc[location_index] == 1:
                if first_alert:
                    figure.axvline(x=self._events_df_file["time"]
                                   .loc[location_index], linewidth=2, linestyle='dashed',
                                   color='r', label='Health_Alert')
                    first_alert = False
                else:
                    figure.axvline(x=self._events_df_file["time"]
                                   .loc[location_index], linewidth=2, linestyle='dashed', color='r')

    def bar_plot(self, name, df_name=None):
        """
        Plotting of Overlapping Bar Graphs:

        :param self:
        :param name: Attribute name
        :param df_name: dataframe
        :return:
        """
        df = df_name
        if df_name is None:
            df = self._mtc.get_stats(name, mlapp_node=None, agent=None, start_time=None, end_time=None)
        node_info = self._mtc.get_nodes()
        if ("keys" in df.columns) and en_plt:
            bins, bins_vert = self.hist_bin_adapt(df)
            name_pipeline, td_matrix = self.align_bins(df)
            if len(bins) > 1:
                fig = plt.figure()
                figure = fig.add_subplot(111)
                color_list = [(1, 0, 0), (0, 0, 1)]
                bins_scale = np.arange(0, len(bins))
                all_pipelines = list(set(name_pipeline))
                for p_idx, pipeline in enumerate(all_pipelines):
                    pipeline_index = [i for i, node_elements in enumerate(node_info)
                                      if node_elements[0] == pipeline]
                    file_index = [i for i, e in enumerate(name_pipeline)
                                  if e == pipeline]
                    for location_index in file_index:
                        # 2D plotting of bar graph
                        colors = tuple((location_index + 1) / (max(file_index) + 1) *
                                       np.array(color_list[p_idx % (len(color_list))]))
                        figure.bar(bins_scale, td_matrix[location_index, :],
                                   color=colors, align='center', alpha=0.1)
                    figure.bar(bins_scale, td_matrix[location_index, :],
                               color=color_list[p_idx % (len(color_list))],
                               align='center', alpha=0.1,
                               label="pipeline: " + node_info[pipeline_index[0]][1] + ", " + node_info[pipeline_index[0]][2])
                figure.set_xticks(bins_scale)
                figure.set_xticklabels(bins_vert)
                figure.tick_params(labelsize=8)
                figure.legend(bbox_to_anchor=(1, 1), prop={'size': 10}, loc=2)
                figure.grid()
                figure.set_title('BarGraph for ' + str(name))
                figure.set_ylabel('normalized bar')
                figure.set_xlabel('bars')

    # Matrix Printing

    def get_matrix_names(self):
        """
        get matrix names:

        :param self:
        :return matrix_name_list:
        """
        matrix_name_list = []
        node_info = self._mtc.get_nodes()
        for filename in self._file_names:
            try:
                df_file = self._matrix_df_file[filename]
                name_list = df_file["Name"].unique()
                node_name = self.file_to_node(filename)
                name_list1 = [[name_el, node_name] for name_el in name_list.tolist()]
                matrix_name_list += name_list1
            except Exception:
                pass
        return matrix_name_list

    def print_matrix(self):
        """
        printing matrix:

        :param self:
        :return:
        """
        for filename in self._file_names:
            try:
                df_file = self._matrix_df_file[filename]
                print("")
                print("")
                print("=======================================================")
                name_list = df_file["Name"].unique()
                for name in name_list:
                    df = df_file[df_file['Name'] == name]
                    df = df.reset_index()
                    num_graphs = df.shape[0]
                    matrix_df = pd.DataFrame()
                    col_num = 0
                    header = []
                    for location_index in range(0, num_graphs):
                        matrix_loc = {}
                        if df["ROW_NAME"].loc[location_index] == "HEADER":
                            header = df["ROW_VALUE"].loc[location_index]
                            col_num = len(header)
                        else:
                            matrix_loc["ROW_NAME"] = df["ROW_NAME"].loc[location_index]
                            matrix_loc["datetime"] = df["datetime"].loc[location_index]
                            for col_index in range(0, col_num):
                                matrix_loc[str(header[col_index])] = \
                                    (df["ROW_VALUE"].loc[location_index])[col_index]
                            matrix_df = matrix_df.append(pd.Series(matrix_loc), ignore_index=True)

                    print('matrix for ' + str(name))
                    if en_tabulate:
                        print(tabulate(matrix_df, headers='keys', tablefmt='psql'))
                    else:
                        print(matrix_df)
            except Exception:
                pass

    def get_matrix_df(self, name):
        """
        getting the DF of a matrix according to the name:

        :param self:
        :return: matrix_df
        """
        matrix_df = pd.DataFrame()
        for filename in self._file_names:
            try:
                node_name = self.file_to_node(filename)
                if (name[1] == node_name):
                    df_file = self._matrix_df_file[filename]
                    name_list = df_file["Name"].unique()
                    if name[0] in name_list:
                        df = df_file[df_file['Name'] == name[0]]
                        df = df.reset_index()
                        num_graphs = df.shape[0]
                        col_num = 0
                        header = []
                        header_names = []
                        for location_index in range(0, num_graphs):
                            matrix_loc = {}
                            if df["ROW_NAME"].loc[location_index] == "HEADER":
                                header = df["ROW_VALUE"].loc[location_index]
                                col_num = len(header)
                            else:
                                matrix_loc["ROW_NAME"] = df["ROW_NAME"].loc[location_index]
                                matrix_loc["datetime"] = df["datetime"].loc[location_index]
                                header_names = []
                                for col_index in range(0, col_num):
                                    matrix_loc[str(header[col_index])] = \
                                        (df["ROW_VALUE"].loc[location_index])[col_index]
                                    header_names.append(str(header[col_index]))
                                matrix_df = matrix_df.append(pd.Series(matrix_loc), ignore_index=True)
                        matrix_df = matrix_df[["ROW_NAME"] + header_names + ["datetime"]]
            except Exception:
                pass
        return matrix_df

    def multigraph_plot(self):
        """
        Plotting of Overlapping MultiGraphs:

        :param self:
        :return:
        """
        for file_name in self._file_names:
            try:
                df = self._multigraph_df_file[file_name]
                name_list = df.Name.unique()
                print("multigraph variables = ", name_list)
                for name in name_list:
                    _df_name = df[df['Name'] == name]
                    _df_name = _df_name.sort_values(by=["time"])  # Time sort
                    _df_name = _df_name.reset_index()
                    len_df = _df_name.shape[0]

                    fig = plt.figure()
                    figure = fig.add_subplot(111)
                    color_list = [(1, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1), (1, 0, 1),
                                  (1, 1, 0), (1, 1, 1)]
                    for grpah_index in range(0, len_df):
                        bins = _df_name["x_series"].iloc[grpah_index]
                        y_values = _df_name["y_series"].iloc[grpah_index]
                        for y_index in range(0, len(y_values)):
                            colors = tuple((grpah_index + 1) / (len_df + 1) *
                                           np.array(color_list[y_index % (len(color_list))]))
                            figure.plot(bins, y_values[y_index]["data"],
                                       color=colors, alpha=0.1, linewidth=3.0)
                    for y_index in range(0, len(y_values)):
                        colors = tuple((grpah_index + 1) / (len_df + 1) *
                                       np.array(color_list[y_index % (len(color_list))]))
                        figure.plot(bins, y_values[y_index]["data"],
                                    color=colors, alpha=0.1, linewidth=3.0,
                                    label=str(y_values[y_index]["label"]))

                    for x_annotation in _df_name["x_annotation"].iloc[0]:
                        figure.axvline(x=x_annotation["value"], linewidth=2, linestyle='dashed',
                                       color='r', label=x_annotation["label"])
                    for y_annotation in _df_name["y_annotation"].iloc[0]:
                        figure.axvline(y=y_annotation["value"], linewidth=2, linestyle='dashed',
                                       color='r', label=y_annotation["label"])
                    figure.legend(bbox_to_anchor=(1, 1), prop={'size': 10}, loc=2)
                    figure.grid()
                    figure.set_title('MultiGraph for ' + str(name))
                    figure.set_ylabel(_df_name["y_title"].iloc[0])
                    figure.set_xlabel(_df_name["x_title"].iloc[0])
            except Exception as err:
                pass  # no multigraphs in this file

    def hist_bin_adapt(self, df):
        """
        The method rounds the bins of the histograms to 4 digits as the MCenter isn't
        For plotting also provide the bins in vertical annotation

        :param self:
        :param df: dataframe
        :return:
        bins:
        bins_vert:
        """
        if "keys" in df.columns:
            bins = df["keys"].loc[0]
            # Adapting continuous histograms to bar graph
            if "inf" in bins[0]:
                bins_vect = bins[0].split(' to ')
                rounded_bins = [bins_vect[0] + "\nto\n" + str(round(float(bins_vect[1]), 4))]
                for index_bins in range(1, len(bins) - 1):
                    bins_vect = bins[index_bins].split(' to ')
                    rounded_bins.append(str(round(float(bins_vect[0]), 4)) + "\nto\n"
                                        + str(round(float(bins_vect[1]), 4)))
                bins_vect = bins[len(bins) - 1].split(' to ')
                rounded_bins.append(str(round(float(bins_vect[0]), 4)) + "\nto\n" + bins_vect[1])
                rounded_bins_flat = [elements_bin.replace("\n", " ") for elements_bin in rounded_bins]
            else:
                rounded_bins = bins
                rounded_bins_flat = bins
            bins_vert = rounded_bins
            bins = rounded_bins_flat
            return bins, bins_vert

    def align_bins(self, df):
        """
        Aligning key bins in the dataframe to the structure of the first location,
        in case of missing bins

        :param self:
        :param df:
        :return:
        name_pipeline1
        td_matrix_loc
        """
        if "keys" in df.columns:
            name_pipeline1 = []
            num_graphs = df.shape[0]
            df_keys = df["keys"]
            bins = df_keys.loc[0]
            num_of_bins = len(bins)
            td_matrix_loc = np.zeros((num_graphs, num_of_bins))
            rounded_bins = bins
            for keys_origin_index in range(0, num_of_bins):
                if ' to ' in bins[keys_origin_index]:
                    bins_vect = bins[keys_origin_index].split(' to ')
                    rounded_bins_vect = bins_vect
                    for index_bin in range(0,2):
                        if 'inf' not in bins_vect[index_bin]:
                            rounded_bins_vect[index_bin] = str(round(float(bins_vect[index_bin]), 4))
                    rounded_bins[keys_origin_index] = rounded_bins_vect[0] + ' to ' + rounded_bins_vect[1]

            for location_index in range(0, num_graphs):
                df_values = []
                for keys_origin_index in range(0, num_of_bins):
                    append_val = 0
                    if ' to ' in bins[keys_origin_index]:
                        bins1 = df_keys.loc[location_index][keys_origin_index]
                        bins1_vect = bins1.split(' to ')
                        rounded_bins1_vect = bins1_vect
                        for index_bin in range(0,2):
                            if 'inf' not in bins1_vect[index_bin]:
                                rounded_bins1_vect[index_bin] = str(round(float(bins1_vect[index_bin]), 4))
                        rounded_bins1 = rounded_bins1_vect[0] + ' to ' + rounded_bins1_vect[1]
                        if rounded_bins[keys_origin_index] == \
                                rounded_bins1:
                            append_val = df["values"].loc[location_index][keys_origin_index]
                    else:
                        for key_index in range(0, len(df_keys.loc[location_index])):
                            if bins[keys_origin_index] == \
                                    df_keys.loc[location_index][key_index]:
                                append_val = df["values"].loc[location_index][key_index]
                    df_values.append(append_val)

                td_matrix_loc[location_index, :] = df_values

                name_pipeline_vect = df["FileName"].loc[location_index].split('-Instance-')
                name_pipeline = (name_pipeline_vect[1].split('-Agent-'))[0]
                name_pipeline1.append(name_pipeline)
            return name_pipeline1, td_matrix_loc

    def file_to_node(self, filename):
        """
        get node name from file name:

        :param self:
        :param filename:
        :return node_name:
        """
        node_info = self._mtc.get_nodes()
        name_pipeline_vect = filename.split('-Instance-')
        name_pipeline = (name_pipeline_vect[1].split('-Agent-'))[0]
        pipeline_index = [i for i, node_elements in enumerate(node_info)
                          if node_elements[0] == name_pipeline]
        return node_info[pipeline_index[0]][2]
