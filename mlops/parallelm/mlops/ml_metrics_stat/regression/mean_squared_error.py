from parallelm.mlops.metrics_constants import RegressionMetrics
from parallelm.mlops.mlops_exception import MLOpsStatisticsException
from parallelm.mlops.stats.single_value import SingleValue
from parallelm.mlops.stats_category import StatCategory


class MeanSquaredError(object):
    """
    Responsibility of this class is basically creating stat object out of mean squared error.
    """

    @staticmethod
    def get_mlops_mse_stat_object(mse):
        """
        Method will create MLOps Single value stat object from numeric real number - mean squared error
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param mse: mean squared error
        :return: Single Value stat object which has mean squared error embedded inside
        """
        single_value = None

        if isinstance(mse, (int, float)):
            single_value = \
                SingleValue() \
                    .name(RegressionMetrics.MEAN_SQUARED_ERROR.value) \
                    .value(mse) \
                    .mode(StatCategory.TIME_SERIES)
        else:
            raise MLOpsStatisticsException \
                ("For outputting mean squared error, mse should be of type numeric but got {}."
                 .format(type(mse)))

        return single_value
