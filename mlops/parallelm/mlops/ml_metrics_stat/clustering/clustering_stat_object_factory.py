import numpy as np

from parallelm.mlops.metrics_constants import ClusteringMetrics
from parallelm.mlops.ml_metrics_stat.ml_stat_object_creator import MLStatObjectCreator
from parallelm.mlops.mlops_exception import MLOpsStatisticsException


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

    @staticmethod
    def get_mlops_calinski_harabaz_score_stat_object(**kwargs):
        """
        Method creates MLOps Single value stat object from numeric real number - calinski harabaz score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: numeric value of calinski harabaz score
        :return: Single Value stat object which has calinski harabaz score embedded inside
        """
        chs = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=ClusteringMetrics.CALINSKI_HARABAZ_SCORE.value,
                                         single_value=chs)

        return single_value, category

    @staticmethod
    def get_mlops_completeness_score_stat_object(**kwargs):
        """
        Method creates MLOps Single value stat object from numeric real number - completeness score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: numeric value of completeness score
        :return: Single Value stat object which has completeness score embedded inside
        """
        cs = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=ClusteringMetrics.COMPLETENESS_SCORE.value,
                                         single_value=cs)

        return single_value, category

    @staticmethod
    def get_mlops_contingency_matrix_stat_object(**kwargs):
        """
        Method creates MLOps Table stat object from ndarray and labels argument coming from kwargs (`true_labels` & `pred_labels`).
        It is not recommended to access this method without understanding table data structure that it is returning.
        :param kwargs: `data` - Array representation of confusion matrix & `true_labels` & `pred_labels` used for representation of contingency matrix
        :return: MLOps Table object generated from array
        """
        cm_nd_array = kwargs.get('data', None)
        true_labels = kwargs.get('true_labels', None)
        pred_labels = kwargs.get('pred_labels', None)

        if true_labels is not None and pred_labels is not None:
            if isinstance(cm_nd_array, np.ndarray) and isinstance(true_labels, list) and isinstance(pred_labels, list):
                if cm_nd_array.shape == (len(true_labels), len(pred_labels)):
                    array_report = list()

                    cm_cols_ordered_string = [str(i) for i in pred_labels]
                    cm_rows_ordered_string = [str(i) for i in true_labels]

                    array_report.append(cm_cols_ordered_string)
                    for index in range(len(cm_nd_array)):
                        row = list(cm_nd_array[index])
                        # adding first col as class it represents
                        row.insert(0, cm_rows_ordered_string[index])

                        array_report.append(row)

                    table_object, category = MLStatObjectCreator \
                        .get_table_value_stat_object(name=ClusteringMetrics.CONTINGENCY_MATRIX.value,
                                                     list_2d=array_report)

                    return table_object, category

                else:
                    raise MLOpsStatisticsException \
                        ("Contingency Matrix.shape = {} and (len(true_labels), len(pred_labels)) = {} does not match"
                         .format(cm_nd_array.shape, (len(true_labels), len(pred_labels))))
            else:
                raise MLOpsStatisticsException \
                    ("Contingency Matrix should be of type numpy nd-array and labels should be of type list")

        else:
            raise MLOpsStatisticsException \
                    (
                    "For outputting contingency matrix labels must be provided using extra `true_labels` & `pred_labels` argument to mlops apis.")

    @staticmethod
    def get_mlops_fowlkes_mallows_score_stat_object(**kwargs):
        """
        Method creates MLOps Single value stat object from numeric real number - fowlkes mallows score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: numeric value of fowlkes mallows score
        :return: Single Value stat object which has fowlkes mallows score embedded inside
        """
        fms = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=ClusteringMetrics.FOWLKES_MALLOWS_SCORE.value,
                                         single_value=fms)

        return single_value, category

    @staticmethod
    def get_mlops_homogeneity_completeness_v_measure_stat_object(**kwargs):
        """
        Method creates MLOps Multiline value stat object from numeric real number - homogeneity, completeness, v_measure
        :param kwargs: list of homogeneity, completeness, v_measure
        :return: Multi Value stat object which has list of homogeneity, completeness, v_measure embedded inside
        """
        homogeneity_completeness_v_measure = kwargs.get('data', None)

        multiline_value, category = MLStatObjectCreator. \
            get_multiline_stat_object(name=ClusteringMetrics.HOMOGENEITY_COMPLETENESS_V_MEASURE.value,
                                      list_value=list(homogeneity_completeness_v_measure),
                                      labels=["Homogeneity", "Completeness", "V Measure"])

        return multiline_value, category

    @staticmethod
    def get_mlops_homogeneity_score_stat_object(**kwargs):
        """
        Method creates MLOps Single value stat object from numeric real number - homogeneity score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: numeric value of homogeneity score
        :return: Single Value stat object which has homogeneity score embedded inside
        """
        hs = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=ClusteringMetrics.HOMOGENEITY_SCORE.value,
                                         single_value=hs)

        return single_value, category

    @staticmethod
    def get_mlops_mutual_info_score_stat_object(**kwargs):
        """
        Method creates MLOps Single value stat object from numeric real number - mutual info score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: numeric value of mutual info score
        :return: Single Value stat object which has mutual info score embedded inside
        """
        mis = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=ClusteringMetrics.MUTUAL_INFO_SCORE.value,
                                         single_value=mis)

        return single_value, category

    @staticmethod
    def get_mlops_normalized_mutual_info_score_stat_object(**kwargs):
        """
        Method creates MLOps Single value stat object from numeric real number - normalized mutual info score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: numeric value of normalized mutual info score
        :return: Single Value stat object which has normalized mutual info score embedded inside
        """
        nmis = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=ClusteringMetrics.NORMALIZED_MUTUAL_INFO_SCORE.value,
                                         single_value=nmis)

        return single_value, category

    @staticmethod
    def get_mlops_silhouette_score_stat_object(**kwargs):
        """
        Method creates MLOps Single value stat object from numeric real number - silhouette score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: numeric value of silhouette score
        :return: Single Value stat object which has silhouette score embedded inside
        """
        ss = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=ClusteringMetrics.SILHOUETTE_SCORE.value,
                                         single_value=ss)

        return single_value, category

    # registry holds name to function mapping. please add __func__ for making static object callable from below getter method.
    registry_name_to_function = {
        ClusteringMetrics.ADJUSTED_MUTUAL_INFO_SCORE: get_mlops_adjusted_mutual_info_score_stat_object.__func__,
        ClusteringMetrics.ADJUSTED_RAND_SCORE: get_mlops_adjusted_rand_score_stat_object.__func__,
        ClusteringMetrics.CALINSKI_HARABAZ_SCORE: get_mlops_calinski_harabaz_score_stat_object.__func__,
        ClusteringMetrics.COMPLETENESS_SCORE: get_mlops_completeness_score_stat_object.__func__,
        ClusteringMetrics.CONTINGENCY_MATRIX: get_mlops_contingency_matrix_stat_object.__func__,
        ClusteringMetrics.FOWLKES_MALLOWS_SCORE: get_mlops_fowlkes_mallows_score_stat_object.__func__,
        ClusteringMetrics.HOMOGENEITY_COMPLETENESS_V_MEASURE: get_mlops_homogeneity_completeness_v_measure_stat_object.__func__,
        ClusteringMetrics.HOMOGENEITY_SCORE: get_mlops_homogeneity_score_stat_object.__func__,
        ClusteringMetrics.MUTUAL_INFO_SCORE: get_mlops_mutual_info_score_stat_object.__func__,
        ClusteringMetrics.NORMALIZED_MUTUAL_INFO_SCORE: get_mlops_normalized_mutual_info_score_stat_object.__func__,
        ClusteringMetrics.SILHOUETTE_SCORE: get_mlops_silhouette_score_stat_object.__func__,
    }

    @staticmethod
    def get_stat_object(name, **kwargs):
        return ClusteringStatObjectFactory.registry_name_to_function[name](**kwargs)
