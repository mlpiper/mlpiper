from parallelm.mlops.metrics_constants import ClassificationMetrics
from parallelm.mlops.mlops_exception import MLOpsStatisticsException
from parallelm.mlops.stats.table import Table


class ClassificationReport(object):
    """
    Responsibility of this class is basically creating table stat object out of classification report string.
    """

    @staticmethod
    def get_mlops_classification_report_table_stat_object(cr):
        """
        Method will create MLOps table value stat object from classification report string.
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param cr: classification report string.
        :return: Table Value stat object which has classification report embedded inside
        """
        table_object = None
        if cr is not None:
            try:
                array_report = list()
                for row in cr.split("\n"):
                    parsed_row = [x for x in row.split("  ") if len(x) > 0]
                    if len(parsed_row) > 0:
                        array_report.append(parsed_row)

                header = list(map(lambda x: x.strip(), array_report[0]))
                first_header_should_be = ['precision', 'recall', 'f1-score', 'support']

                assert header == first_header_should_be, \
                    "header {} is not matching expecting classification header pattern {}".format(header,
                                                                                                  first_header_should_be)

                table_object = Table().name(ClassificationMetrics.CLASSIFICATION_REPORT.value).cols(header)

                for index in range(1, len(array_report)):
                    row_title = array_report[index][0].strip()
                    row_value = list(map(lambda x: x.strip(), array_report[index][1:]))

                    table_object.add_row(row_title, row_value)

            except Exception as e:
                raise MLOpsStatisticsException(
                    "error happened while outputting classification report as table. Got classification string {}.\n error: {}"
                        .format(cr, e))
        else:
            raise MLOpsStatisticsException \
                ("cr object for outputting classification report cannot be None.")

        return table_object
