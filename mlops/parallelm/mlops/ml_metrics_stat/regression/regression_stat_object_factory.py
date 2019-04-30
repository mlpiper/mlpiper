import numpy as np

from parallelm.mlops.metrics_constants import RegressionMetrics
from parallelm.mlops.ml_metrics_stat.ml_stat_object_creator import MLStatObjectCreator


class RegressionStatObjectFactory(object):
    """
    Responsibility of this class is basically creating stat object for regression stat.
    """

    @staticmethod
    def get_mlops_explained_variance_score_stat_object(**kwargs):
        """
        Method creates MLOps Single/Multiline value stat object from numeric real number - explained variance score or list of variances
        :param kwargs: explained variance score or array of variance scores.
        :return: Single/Multiline Value stat object which has explained variance score embedded inside
        """
        evs = kwargs.get('data', None)

        if isinstance(evs, list) or isinstance(evs, np.ndarray):
            multiline_value, category = MLStatObjectCreator. \
                get_multiline_stat_object(name=RegressionMetrics.EXPLAINED_VARIANCE_SCORE.value,
                                          list_value=evs)

            return multiline_value, category

        # if it is not list then it has to be single value.
        else:
            single_value, category = MLStatObjectCreator. \
                get_single_value_stat_object(name=RegressionMetrics.EXPLAINED_VARIANCE_SCORE.value,
                                             single_value=evs)

            return single_value, category

    @staticmethod
    def get_mlops_mean_absolute_error_stat_object(**kwargs):
        """
        Method creates MLOps Single/Multiline value stat object from numeric real number - mean absolute error; or list of errors
        :param kwargs: mean absolute error or array of mean absolute errors
        :return: Single/Multiline Value stat object which has mean absolute error embedded inside
        """
        mae = kwargs.get('data', None)

        if isinstance(mae, list) or isinstance(mae, np.ndarray):
            multiline_value, category = MLStatObjectCreator. \
                get_multiline_stat_object(name=RegressionMetrics.MEAN_ABSOLUTE_ERROR.value,
                                          list_value=mae)

            return multiline_value, category

        # if it is not list then it has to be single value.
        else:
            single_value, category = MLStatObjectCreator. \
                get_single_value_stat_object(name=RegressionMetrics.MEAN_ABSOLUTE_ERROR.value,
                                             single_value=mae)

            return single_value, category

    @staticmethod
    def get_mlops_mean_squared_error_stat_object(**kwargs):
        """
        Method creates MLOps Single/Multiline value stat object from numeric real number - mean squared error; or list of errors
        :param kwargs: mean squared error or array of mean square errors
        :return: Single/Multiline Value stat object which has mean squared error embedded inside
        """
        mse = kwargs.get('data', None)

        if isinstance(mse, list) or isinstance(mse, np.ndarray):
            multiline_value, category = MLStatObjectCreator. \
                get_multiline_stat_object(name=RegressionMetrics.MEAN_SQUARED_ERROR.value,
                                          list_value=mse)

            return multiline_value, category

        # if it is not list then it has to be single value.
        else:
            single_value, category = MLStatObjectCreator. \
                get_single_value_stat_object(name=RegressionMetrics.MEAN_SQUARED_ERROR.value,
                                             single_value=mse)

            return single_value, category

    @staticmethod
    def get_mlops_mean_squared_log_error_stat_object(**kwargs):
        """
        Method creates MLOps Single/Multiline value stat object from numeric real number - mean squared log error; or list of errors
        :param kwargs: mean squared log error or array of mean squared log errors
        :return: Single/Multiline Value stat object which has mean squared log error embedded inside
        """
        msle = kwargs.get('data', None)
        if isinstance(msle, list) or isinstance(msle, np.ndarray):
            multiline_value, category = MLStatObjectCreator. \
                get_multiline_stat_object(name=RegressionMetrics.MEAN_SQUARED_LOG_ERROR.value,
                                          list_value=msle)

            return multiline_value, category

        # if it is not list then it has to be single value.
        else:
            single_value, category = MLStatObjectCreator. \
                get_single_value_stat_object(name=RegressionMetrics.MEAN_SQUARED_LOG_ERROR.value,
                                             single_value=msle)

            return single_value, category

    @staticmethod
    def get_mlops_median_absolute_error_stat_object(**kwargs):
        """
        Method creates MLOps Single value stat object from numeric real number - median absolute error
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: mean absolute error
        :return: Single Value stat object which has median absolute error embedded inside
        """
        mae = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=RegressionMetrics.MEDIAN_ABSOLUTE_ERROR.value,
                                         single_value=mae)

        return single_value, category

    @staticmethod
    def get_mlops_r2_score_stat_object(**kwargs):
        """
        Method creates MLOps Single/Multiline value stat object from numeric real number - r2 score; or list of r2 score
        :param kwargs: r2 score or array r2 score
        :return: Single/Multiline Value stat object which has r2 score embedded inside
        """
        r2_score = kwargs.get('data', None)
        if isinstance(r2_score, list) or isinstance(r2_score, np.ndarray):
            multiline_value, category = MLStatObjectCreator. \
                get_multiline_stat_object(name=RegressionMetrics.R2_SCORE.value,
                                          list_value=r2_score)

            return multiline_value, category

        # if it is not list then it has to be single value.
        else:
            single_value, category = MLStatObjectCreator. \
                get_single_value_stat_object(name=RegressionMetrics.R2_SCORE.value,
                                             single_value=r2_score)

            return single_value, category

    # registry holds name to function mapping. please add __func__ for making static object callable from below getter method.
    registry_name_to_function = {
        RegressionMetrics.EXPLAINED_VARIANCE_SCORE: get_mlops_explained_variance_score_stat_object.__func__,
        RegressionMetrics.MEAN_ABSOLUTE_ERROR: get_mlops_mean_absolute_error_stat_object.__func__,
        RegressionMetrics.MEAN_SQUARED_ERROR: get_mlops_mean_squared_error_stat_object.__func__,
        RegressionMetrics.MEAN_SQUARED_LOG_ERROR: get_mlops_mean_squared_log_error_stat_object.__func__,
        RegressionMetrics.MEDIAN_ABSOLUTE_ERROR: get_mlops_median_absolute_error_stat_object.__func__,
        RegressionMetrics.R2_SCORE: get_mlops_r2_score_stat_object.__func__
    }

    @staticmethod
    def get_stat_object(name, **kwargs):
        return RegressionStatObjectFactory.registry_name_to_function[name](**kwargs)
