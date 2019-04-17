from parallelm.mlops.metrics_constants import ClassificationMetrics
from parallelm.mlops.mlops_exception import MLOpsStatisticsException
from parallelm.mlops.stats.single_value import SingleValue
from parallelm.mlops.stats_category import StatCategory


class AUC(object):
    """
    Responsibility of this class is basically creating stat object out of auc score.
    """

    @staticmethod
    def get_mlops_auc_stat_object(auc):
        """
        Method will create MLOps Single value stat object from numeric real number - auc score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param auc: numeric value of auc
        :return: Single Value stat object which has auc score embedded inside
        """
        single_value = None

        if isinstance(auc, (int, float)):
            single_value = \
                SingleValue() \
                    .name(ClassificationMetrics.AUC.value) \
                    .value(auc) \
                    .mode(StatCategory.TIME_SERIES)
        else:
            raise MLOpsStatisticsException \
                ("For outputting accuracy score, accuracy should be of type numeric but got {}."
                 .format(type(auc)))

        return single_value
