"""
MLOps supports propagating a table up to the PM system. This file contains table definition methods.
"""

import os
import copy
import six

from parallelm.mlops.mlops_env_constants import MLOpsEnvConstants
from parallelm.mlops.stats.mlops_stat import MLOpsStat
from parallelm.mlops.stats_category import StatGraphType, StatsMode

from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats.mlops_stat_getter import MLOpsStatGetter

from parallelm.protobuf import InfoType_pb2

from collections import OrderedDict


def verify_table_from_list_of_lists(tbl_data):
    """
    Verify that tbl_data is in the format expected
    :param tbl_data:
    :return: Throws an exception in case of badly formatted table data
    """
    if not isinstance(tbl_data, list):
        raise MLOpsException("Table data is not in the format of list of lists")

    row_idx = 0
    for item in tbl_data:
        if not isinstance(item, list):
            raise MLOpsException("Each item of tbl_data should be a list - row {} is not a list".format(row_idx))
        row_idx += 1

    if len(tbl_data) < 2:
        raise MLOpsException("Table data must contain at least column names and one row of data")

    nr_cols = len(tbl_data[0])
    row_idx = 0
    for row in tbl_data:
        if len(row) != nr_cols:
            raise MLOpsException("Row {} items number is {} which is not equal to number of columns provided {}".
                                 format(row_idx, len(row), nr_cols))
        row_idx += 1


def is_list_of_lists(tbl_data):
    """
    Check if tbl_data is list of lists
    :param tbl_data:
    :return:
    """
    if not isinstance(tbl_data, list):
        return False

    return all(isinstance(item, list) for item in tbl_data)


class Table(MLOpsStatGetter):
    """
    A container for tabular/matrix data object.
    By using Table it is possible to report tabular data to MLOps.

    :Example:

    >>> tbl = Table().name("MyTable").cols(["Iteration number", "Error"])
    >>> for iter_id in range(0, 100):
    >>>     error_value = do_calculation()
    >>>     row_name = join(["row num",iter_id])
    >>>     tbl.add_row(row_name, error_value])
    >>> mlops.set_stat(tbl)

    The above example creates a Table object and populates it with data.
    The call .name("MyTable") sets the name of the table. This name will be used when the table
    is displayed in the MLOps UI. The .cols() method will set the names of the cols, and also
    the expected number of columns.

    By calling .add_row() the user can add rows to the table, where the argument provided is the row name and a list of
    data items to add to the row. The number of data items in the list should be the same as the number of columns.

    Finally, the call to mlops.set_stat(tbl) generates the table statistics and pushes it to MLOps
    for further processing.

    """

    def __init__(self):
        self._name = "Table"
        self._cols_names = []
        self._rows_names = []
        self._tbl_rows = []

    def name(self, name):
        """
        Set the name of the table/matrix

        :param name: table name
        :return: the Table object
        """
        self._name = name
        return self

    def _to_dict(self):

        tbl_dict = OrderedDict()

        row_names = list(self._rows_names)

        for line in self._tbl_rows:
            if len(row_names) > 0:
                row_name = row_names.pop(0)
            else:
                row_name = ""

            tbl_dict[row_name] = OrderedDict()
            col_idx = 0
            for col in line:
                tbl_dict[row_name][self._cols_names[col_idx]] = col
                col_idx += 1
        return tbl_dict

    def columns_names(self, name_list):
        """
        Set the table column names

        :param name_list: List of names
        :return: self
        """
        if not isinstance(name_list, list):
            raise MLOpsException("Columns names should be provided as a list")

        if len(self._tbl_rows) > 0:
            row_len = len(self._tbl_rows[0])
            if len(name_list) != row_len:
                raise MLOpsException("Number of columns names provided must match number of columns")
        self._cols_names = name_list
        return self

    def cols(self, name_list):
        """
        Set the table column names

        :param name_list: List of names
        :return: self
        """
        return self.columns_names(name_list)

    def add_row(self, arg1, arg2=None):
        """
        Add a row to the table.

        :param arg1: Name of the row, or list of data items
        :param arg2: List of data items (if argument 1 was provided)
        :return: self

        """
        row_name = ""
        row_data = []
        if isinstance(arg1, six.string_types):
            # The case where we get the name of the row as the first argument
            row_name = copy.deepcopy(arg1)
            if arg2 is None:
                raise MLOpsException("no data provided for row")
            if not isinstance(arg2, list):
                raise MLOpsException("Data should be provided as a list")
            row_data = copy.deepcopy(arg2)
        elif isinstance(arg1, list):
            # The case where we get only data without the line/row name
            row_data = copy.deepcopy(arg1)
        else:
            raise MLOpsException("Either provide row_name, data or just data")

        if len(self._tbl_rows) > 0:
            if len(self._tbl_rows[0]) != len(row_data):
                raise MLOpsException("row length must be equal to length of previously provided rows")

        self._tbl_rows.append(row_data)
        self._rows_names.append(row_name)
        return self

    def add_rows(self, rows):
        if not isinstance(rows, list):
            raise MLOpsException("Rows data should be provided as list of lists")

        for row in rows:
            self.add_row(row)

        return self

    def _to_semi_json(self, escape=True):
        """
        Convert to string that can be inserted into database and visualized by UI.

        :param escape: indicates whether to escape double quotes
        When escape=True, format is:
        "{\"row1\":{\"col1\":1.0,\"col2\":2.0}, \"row2\":{\"col1\":3.0,\"col2\":4.0}}"

        When escape=False, format is:
        "{"row1":{"col1":1.0,"col2":2.0}, "row2":{"col1":3.0,"col2":4.0}}"

        :return: string with table in the above format
        """

        # This is a workaround for REF-4151. It adds a unique label to each row that is removed by the UI.
        unique_label = True
        if os.environ.get(MLOpsEnvConstants.MLOPS_MODE, "STAND_ALONE") is "STAND_ALONE":
            unique_label = False

        num_rows = len(self._tbl_rows)
        if len(self._rows_names) != num_rows:
            raise MLOpsException("Number of row names {} must match number of rows {}"
                                 .format(self._rows_names, num_rows))

        num_col_names = len(self._cols_names)
        num_cols = 0
        if num_rows > 0:
            num_cols = len(self._tbl_rows[0])

        if num_cols > 0 and num_col_names > 0 and num_cols != num_col_names:
            raise MLOpsException("Number of column names {} must match number of columns {}"
                                 .format(num_col_names, num_cols))

        uniq_row_label_start = ""
        uniq_row_label_end = ""
        if unique_label:
            uniq_row_label_start = "__REMOVE"
            uniq_row_label_end = "LABEL__"

        if escape:
            row_name_start = '\\\"' + uniq_row_label_start
            row_name_end = '\\\":{'
            col_name_start = '\\\"'
            col_name_end = '\\\":'
            string_entry_start = '\\"'
            string_entry_end = '\\\"'
        else:
            row_name_start = '"' + uniq_row_label_start
            row_name_end = '":{'
            col_name_start = '"'
            col_name_end = '":'
            string_entry_start = '"'
            string_entry_end = '"'

        s = '{'
        for i in range(num_rows):
            s += row_name_start
            if unique_label:
                s += str(i) + uniq_row_label_end
            s += self._rows_names[i]
            s += row_name_end
            for j in range(num_cols):
                s += col_name_start
                if num_col_names > 0:
                    s += self._cols_names[j]
                s += col_name_end
                if isinstance(self._tbl_rows[i][j], six.string_types):
                    s += string_entry_start
                    s += self._tbl_rows[i][j]
                    s += string_entry_end
                else:
                    s += str(self._tbl_rows[i][j])
                if j < (num_cols - 1):
                    s += ', '
                else:
                    s += '}'
            if i < (num_rows - 1):
                s += ', '
            else:
                s += '}'

        return s

    def get_mlops_stat(self, model_id):

        if len(self._tbl_rows) == 0:
            raise MLOpsException("No rows data found in table object")

        tbl_data = self._to_semi_json(escape=False)
        semi_json = self._to_semi_json()

        mlops_stat = MLOpsStat(name=self._name,
                               stat_type=InfoType_pb2.General,
                               graph_type=StatGraphType.MATRIX,
                               mode=StatsMode.Instant,
                               data=tbl_data,
                               string_data=semi_json,
                               json_data_dump = tbl_data,
                               model_id=model_id)
        return mlops_stat
