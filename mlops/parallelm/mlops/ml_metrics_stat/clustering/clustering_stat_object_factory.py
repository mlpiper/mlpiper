from parallelm.mlops.metrics_constants import ClusteringMetrics
from parallelm.mlops.ml_metrics_stat.ml_stat_object_creator import MLStatObjectCreator


class ClusteringStatObjectFactory(object):
    """
    Responsibility of this class is basically creating stat object for clustering stat.
    """

    @staticmethod
    def get_mlops_adjusted_mutual_info_score_stat_object(**kwargs):
        """
        Method creates MLOps Single value stat object from numeric real number - adjusted mutual info score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: numeric value of adjusted mutual info score
        :return: Single Value stat object which has adjusted mutual info score embedded inside
        """
        amis = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=ClusteringMetrics.ADJUSTED_MUTUAL_INFO_SCORE.value,
                                         single_value=amis)

        return single_value, category

    @staticmethod
    def get_mlops_adjusted_rand_score_stat_object(**kwargs):
        """
        Method creates MLOps Single value stat object from numeric real number - adjusted rand score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: numeric value of adjusted rand score
        :return: Single Value stat object which has adjusted rand score embedded inside
        """
        ars = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=ClusteringMetrics.ADJUSTED_RAND_SCORE.value,
                                         single_value=ars)

        return single_value, category

    # registry holds name to function mapping. please add __func__ for making static object callable from below getter method.
    registry_name_to_function = {
        ClusteringMetrics.ADJUSTED_MUTUAL_INFO_SCORE: get_mlops_adjusted_mutual_info_score_stat_object.__func__,
        ClusteringMetrics.ADJUSTED_RAND_SCORE: get_mlops_adjusted_rand_score_stat_object.__func__
    }

    @staticmethod
    def get_stat_object(name, **kwargs):
        return ClusteringStatObjectFactory.registry_name_to_function[name](**kwargs)
