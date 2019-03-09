import pandas as pd


class JsonToDf:
    """
    This class converts JSONs from the MCenter time capture to pandas Dataframes
    """

    @staticmethod
    def parse_bar(input_entry):
        """
        This function extracts the bargraph line.
        each row is spanned:
        Name, time, keys, values

        :param input_entry: line to parse
        :return:
        parsed_entry
        """
        keys_list = []
        value_list = []
        for key in sorted(input_entry.keys()):
            resp_in = input_entry[key]
            for elements in resp_in:
                for key_in in sorted(elements.keys()):
                    keys_list.append(key_in)
                    value_list.append(elements[key_in])
        keys = keys_list
        values = value_list
        return keys, values

    @staticmethod
    def parse_multiline(input_entry):
        """
        This function extracts the linegraph/multilinegraph/heatmap line.
        each row is spanned:
        Name, time, keys, values

        :param input_entry: line to parse
        :return:
        parsed_entry
        """
        keys = sorted(input_entry.keys())
        value_list = []
        for key_in in sorted(input_entry.keys()):
            value_list.append(input_entry[key_in])
        values = value_list
        return keys, values

    @staticmethod
    def parse_multigraph(parsed_entry, input_entry):
        """
        This function extracts the multigraph line.
        each row is spanned:
        Name, time, keys, values

        :param parsed_entry: Key value that contains the result
        :param input_entry: line to parse
        :return:
        parsed_entry
        """
        for key_in in sorted(input_entry.keys()):
            parsed_entry[key_in] = input_entry[key_in]
        return parsed_entry

    @staticmethod
    def parse_matrix(parsed_entry, input_entry):
        """
        This function extracts the matrix line.
        each row is spanned:
        Name, time, keys, values

        :param parsed_entry: Key value that contains the result
        :param input_entry: line to parse
        :return:
        parsed_entry
        """
        col_head = True
        matrix_line_df = pd.DataFrame()
        for key_in in sorted(input_entry.keys()):
            if col_head:
                col_head = False
                parsed_entry["ROW_NAME"] = "HEADER"
                parsed_entry["ROW_VALUE"] = []
                for key_col in sorted(input_entry[key_in].keys()):
                    parsed_entry["ROW_VALUE"].append(key_col)
        matrix_line_df = matrix_line_df.append(pd.Series(parsed_entry), ignore_index=True)
        for key_in in sorted(input_entry.keys()):
            parsed_entry["ROW_NAME"] = key_in
            parsed_entry["ROW_VALUE"] = []
            for key_col in sorted(input_entry[key_in].keys()):
                parsed_entry["ROW_VALUE"].append(input_entry[key_in][key_col])
            matrix_line_df = matrix_line_df.append(pd.Series(parsed_entry), ignore_index=True)
        return matrix_line_df
