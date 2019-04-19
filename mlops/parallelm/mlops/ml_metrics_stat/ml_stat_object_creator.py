from parallelm.mlops.mlops_exception import MLOpsStatisticsException
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
        Create Table Value stat object from list of list. Where first element of 2d list will be header. And from remaining lists, list's first index will be Row's header.
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
