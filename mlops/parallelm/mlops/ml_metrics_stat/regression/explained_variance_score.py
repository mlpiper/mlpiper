from parallelm.mlops.metrics_constants import RegressionMetrics
from parallelm.mlops.mlops_exception import MLOpsStatisticsException
from parallelm.mlops.stats.single_value import SingleValue
from parallelm.mlops.stats_category import StatCategory


class ExplainedVarianceScore(object):
    """
    Responsibility of this class is basically creating stat object out of explained variance score.
    """

    @staticmethod
    def get_mlops_evs_stat_object(evs):
        """
        Method will create MLOps Single value stat object from numeric real number - e score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param evs: explained variance score
        :return: Single Value stat object which has explained variance score embedded inside
        """
        single_value = None

        if isinstance(evs, (int, float)):
            single_value = \
                SingleValue() \
                    .name(RegressionMetrics.EXPLAINED_VARIANCE_SCORE.value) \
                    .value(evs) \
                    .mode(StatCategory.TIME_SERIES)
        else:
            raise MLOpsStatisticsException \
                ("For outputting explained variance score, evs should be of type numeric but got {}."
                 .format(type(evs)))

        return single_value
