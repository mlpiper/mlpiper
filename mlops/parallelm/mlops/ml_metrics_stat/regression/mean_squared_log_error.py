from parallelm.mlops.metrics_constants import RegressionMetrics
from parallelm.mlops.mlops_exception import MLOpsStatisticsException
from parallelm.mlops.stats.single_value import SingleValue
from parallelm.mlops.stats_category import StatCategory


class MeanSquaredLogError(object):
    """
    Responsibility of this class is basically creating stat object out of mean squared log error.
    """

    @staticmethod
    def get_mlops_msle_stat_object(msle):
        """
        Method will create MLOps Single value stat object from numeric real number - mean squared log error
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param msle: mean squared log error
        :return: Single Value stat object which has mean squared log error embedded inside
        """
        single_value = None

        if isinstance(msle, (int, float)):
            single_value = \
                SingleValue() \
                    .name(RegressionMetrics.MEAN_SQUARED_LOG_ERROR.value) \
                    .value(msle) \
                    .mode(StatCategory.TIME_SERIES)
        else:
            raise MLOpsStatisticsException \
                ("For outputting mean squared error, mse should be of type numeric but got {}."
                 .format(type(msle)))

        return single_value
