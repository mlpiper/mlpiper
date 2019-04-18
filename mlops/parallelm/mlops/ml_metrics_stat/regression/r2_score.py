from parallelm.mlops.metrics_constants import RegressionMetrics
from parallelm.mlops.mlops_exception import MLOpsStatisticsException
from parallelm.mlops.stats.single_value import SingleValue
from parallelm.mlops.stats_category import StatCategory


class R2Score(object):
    """
    Responsibility of this class is basically creating stat object out of r2 score.
    """

    @staticmethod
    def get_mlops_r2_score_stat_object(r2_score):
        """
        Method will create MLOps Single value stat object from numeric real number - r2 score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param r2_score: r2 score
        :return: Single Value stat object which has r2 score embedded inside
        """
        single_value = None

        if isinstance(r2_score, (int, float)):
            single_value = \
                SingleValue() \
                    .name(RegressionMetrics.R2_SCORE.value) \
                    .value(r2_score) \
                    .mode(StatCategory.TIME_SERIES)
        else:
            raise MLOpsStatisticsException \
                ("For outputting r2 score, r2_score should be of type numeric but got {}."
                 .format(type(r2_score)))

        return single_value
