from parallelm.mlops.metrics_constants import ClassificationMetrics
from parallelm.mlops.mlops_exception import MLOpsStatisticsException
from parallelm.mlops.stats.single_value import SingleValue
from parallelm.mlops.stats_category import StatCategory


class AccuracyScore(object):
    """
    Responsibility of this class is basically creating stat object out of accuracy score.
    """

    @staticmethod
    def get_mlops_accuracy_stat_object(accuracy_score):
        """
        Method will create MLOps Single value stat object from numeric real number - accuracy score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param accuracy_score: numeric value of accuracy
        :return: Single Value stat object which has accuracy score embedded inside
        """
        single_value = None

        if isinstance(accuracy_score, (int, float)):
            single_value = \
                SingleValue() \
                    .name(ClassificationMetrics.ACCURACY_SCORE.value) \
                    .value(accuracy_score) \
                    .mode(StatCategory.TIME_SERIES)
        else:
            raise MLOpsStatisticsException \
                ("For outputting accuracy score, accuracy should be of type numeric but got {}."
                 .format(type(accuracy_score)))

        return single_value
