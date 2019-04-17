from parallelm.mlops.metrics_constants import ClassificationMetrics
from parallelm.mlops.mlops_exception import MLOpsStatisticsException
from parallelm.mlops.stats.single_value import SingleValue
from parallelm.mlops.stats_category import StatCategory


class AveragePrecisionScore(object):
    """
    Responsibility of this class is basically creating stat object out of average precision score.
    """

    @staticmethod
    def get_mlops_aps_stat_object(aps):
        """
        Method will create MLOps Single value stat object from numeric real number - average precision score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param aps: numeric value of average precision score
        :return: Single Value stat object which has aps score embedded inside
        """
        single_value = None

        if isinstance(aps, (int, float)):
            single_value = \
                SingleValue() \
                    .name(ClassificationMetrics.AVERAGE_PRECISION_SCORE.value) \
                    .value(aps) \
                    .mode(StatCategory.TIME_SERIES)
        else:
            raise MLOpsStatisticsException \
                ("For outputting average precision score, value should be of type numeric but got {}."
                 .format(type(aps)))

        return single_value
