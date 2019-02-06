import six

from parallelm.mlops.stats_category import StatGraphType
from parallelm.mlops.mlops_exception import MLOpsException


class DataToJson:
    """
    Converts statistics types into json. This is used to serialize statistics reported to MLOps.
    """

    @staticmethod
    def _tbl_to_json(tbl):
        if len(tbl) < 2:
            raise MLOpsException("Table must contain at least 2 lines (first line is column names)")

        # Remove the first item in the col_names
        col_names = tbl.pop(0)

        # Pop the first item which is not used
        col_names.pop(0)

        tbl_map = {}

        for line in tbl:
            row_name = line.pop(0)
            tbl_map[str(row_name)] = {}
            col_idx = 0
            for col in line:
                tbl_map[str(row_name)][str(col_names[col_idx])] = col
                col_idx += 1
        return tbl_map

    @staticmethod
    def _multi_line_to_json(multi_line):
        multi_line_map = {}


    @staticmethod
    def json(data, graph_type):

        if graph_type == StatGraphType.LINEGRAPH:
            if isinstance(data, (six.integer_types, float)):
                return {"0": data}

        elif graph_type == StatGraphType.MATRIX:
            return DataToJson._tbl_to_json(data)

        elif graph_type == StatGraphType.MULTILINEGRAPH:
            return DataToJson._multi_line_to_json(data)

        raise MLOpsException("data type or stat type not supported")
