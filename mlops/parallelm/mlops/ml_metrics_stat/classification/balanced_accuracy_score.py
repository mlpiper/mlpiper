from parallelm.mlops.metrics_constants import ClassificationMetrics
from parallelm.mlops.mlops_exception import MLOpsStatisticsException
from parallelm.mlops.stats.single_value import SingleValue
from parallelm.mlops.stats_category import StatCategory


class BalancedAccuracyScore(object):
    """
    Responsibility of this class is basically creating stat object out of balanced accuracy score.
    """

    @staticmethod
    def get_mlops_balanced_accuracy_stat_object(balanced_accuracy_score):
        """
        Method will create MLOps Single value stat object from numeric real number - balanced accuracy score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param balanced_accuracy_score: numeric value of balanced accuracy
        :return: Single Value stat object which has accuracy score embedded inside
        """
        single_value = None

        if isinstance(balanced_accuracy_score, (int, float)):
            single_value = \
                SingleValue() \
                    .name(ClassificationMetrics.BALANCED_ACCURACY_SCORE.value) \
                    .value(balanced_accuracy_score) \
                    .mode(StatCategory.TIME_SERIES)
        else:
            raise MLOpsStatisticsException \
                ("For outputting balanced accuracy score, accuracy should be of type numeric but got {}."
                 .format(type(balanced_accuracy_score)))

        return single_value
