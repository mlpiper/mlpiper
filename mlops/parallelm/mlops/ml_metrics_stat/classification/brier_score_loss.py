from parallelm.mlops.metrics_constants import ClassificationMetrics
from parallelm.mlops.mlops_exception import MLOpsStatisticsException
from parallelm.mlops.stats.single_value import SingleValue
from parallelm.mlops.stats_category import StatCategory


class BrierScoreLoss(object):
    """
    Responsibility of this class is basically creating stat object out of brier score loss.
    """

    @staticmethod
    def get_mlops_bsl_stat_object(bsl):
        """
        Method will create MLOps Single value stat object from numeric real number - brier score loss
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param bsl: numeric value of brier score loss
        :return: Single Value stat object which has brier score embedded inside
        """
        single_value = None

        if isinstance(bsl, (int, float)):
            single_value = \
                SingleValue() \
                    .name(ClassificationMetrics.BRIER_SCORE_LOSS.value) \
                    .value(bsl) \
                    .mode(StatCategory.TIME_SERIES)
        else:
            raise MLOpsStatisticsException \
                ("For outputting brier score, score should be of type numeric but got {}."
                 .format(type(bsl)))

        return single_value
