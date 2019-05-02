import pytest
from sklearn import metrics

from parallelm.mlops import mlops as mlops
from parallelm.mlops.metrics_constants import ClusteringMetrics
from parallelm.mlops.mlops_exception import MLOpsStatisticsException
from parallelm.mlops.mlops_mode import MLOpsMode


def test_mlops_adjusted_mutual_info_score_apis():
    mlops.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    labels_pred = [1, 0, 1, 2, 3, 0]
    labels_actual = [0, 1, 0, 1, 3, 1]

    amis = metrics.adjusted_mutual_info_score(labels_actual, labels_pred)

    # first way
    mlops.set_stat(ClusteringMetrics.ADJUSTED_MUTUAL_INFO_SCORE, amis)

    # second way
    mlops.metrics.adjusted_mutual_info_score(labels_true=labels_actual, labels_pred=labels_pred)

    # should throw error if not numeric number is provided
    with pytest.raises(MLOpsStatisticsException):
        mlops.set_stat(ClusteringMetrics.ADJUSTED_MUTUAL_INFO_SCORE, [1, 2, 3])

    # should throw error if labels predicted is different length than actuals
    with pytest.raises(ValueError):
        labels_pred_missing_values = [0, 0, 0, 1]
        mlops.metrics.adjusted_mutual_info_score(labels_true=labels_actual, labels_pred=labels_pred_missing_values)

    mlops.done()


def test_mlops_adjusted_rand_score_apis():
    mlops.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    labels_pred = [1, 0, 1, 2, 3, 0]
    labels_actual = [0, 1, 0, 1, 3, 1]

    ars = metrics.adjusted_rand_score(labels_actual, labels_pred)

    # first way
    mlops.set_stat(ClusteringMetrics.ADJUSTED_RAND_SCORE, ars)

    # second way
    mlops.metrics.adjusted_rand_score(labels_true=labels_actual, labels_pred=labels_pred)

    # should throw error if not numeric number is provided
    with pytest.raises(MLOpsStatisticsException):
        mlops.set_stat(ClusteringMetrics.ADJUSTED_RAND_SCORE, [1, 2, 3])

    # should throw error if labels predicted is different length than actuals
    with pytest.raises(ValueError):
        labels_pred_missing_values = [0, 0, 0, 1]
        mlops.metrics.adjusted_rand_score(labels_true=labels_actual, labels_pred=labels_pred_missing_values)

    mlops.done()


def test_mlops_calinski_harabaz_score_apis():
    mlops.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    X = [[1, 2], [1, 3], [1, 2], [2, 4], [4, 5], [9, 9]]
    labels_pred = [1, 0, 1, 2, 3, 0]

    chs = metrics.calinski_harabaz_score(X, labels_pred)

    # first way
    mlops.set_stat(ClusteringMetrics.CALINSKI_HARABAZ_SCORE, chs)

    # second way
    mlops.metrics.calinski_harabaz_score(X=X, labels=labels_pred)

    # should throw error if not numeric number is provided
    with pytest.raises(MLOpsStatisticsException):
        mlops.set_stat(ClusteringMetrics.CALINSKI_HARABAZ_SCORE, [1, 2, 3])

    # should throw error if labels predicted is different length than actuals
    with pytest.raises(ValueError):
        labels_pred_missing_values = [0, 0, 0, 1]
        mlops.metrics.calinski_harabaz_score(X=X, labels=labels_pred_missing_values)

    mlops.done()


def test_mlops_completeness_score_apis():
    mlops.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    labels_pred = [1, 0, 1, 2, 3, 0]
    labels_actual = [0, 1, 0, 1, 3, 1]

    cs = metrics.completeness_score(labels_actual, labels_pred)

    # first way
    mlops.set_stat(ClusteringMetrics.COMPLETENESS_SCORE, cs)

    # second way
    mlops.metrics.completeness_score(labels_true=labels_actual, labels_pred=labels_pred)

    # should throw error if not numeric number is provided
    with pytest.raises(MLOpsStatisticsException):
        mlops.set_stat(ClusteringMetrics.COMPLETENESS_SCORE, [1, 2, 3])

    # should throw error if labels predicted is different length than actuals
    with pytest.raises(ValueError):
        labels_pred_missing_values = [0, 0, 0, 1]
        mlops.metrics.completeness_score(labels_true=labels_actual, labels_pred=labels_pred_missing_values)

    mlops.done()


def test_mlops_contingency_matrix_apis():
    mlops.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    labels_pred = [1, 0, 1, 2, 3, 0]
    labels_actual = [0, 1, 0, 1, 3, 1]

    # list of sorted labels. i.e. [0, 1, 2, ..]
    pred_labels = sorted(set(labels_pred))
    true_labels = sorted(set(labels_actual))

    cm = metrics.cluster.contingency_matrix(labels_actual, labels_pred, eps=None)

    # first way
    mlops.set_stat(ClusteringMetrics.CONTINGENCY_MATRIX,
                   data=cm,
                   true_labels=true_labels,
                   pred_labels=pred_labels)

    # should throw error if true and pred labels list is missing
    with pytest.raises(MLOpsStatisticsException):
        mlops.set_stat(ClusteringMetrics.CONTINGENCY_MATRIX,
                       data=cm)

    # should throw error if true labels list is missing
    with pytest.raises(MLOpsStatisticsException):
        mlops.set_stat(ClusteringMetrics.CONTINGENCY_MATRIX,
                       data=cm,
                       pred_labels=pred_labels)

    # should throw error if pred labels list is missing
    with pytest.raises(MLOpsStatisticsException):
        mlops.set_stat(ClusteringMetrics.CONTINGENCY_MATRIX,
                       data=cm,
                       true_labels=true_labels)

    # second way
    mlops.metrics.cluster.contingency_matrix(labels_actual, labels_pred)

    mlops.done()


def test_mlops_fowlkes_mallows_score_apis():
    mlops.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    labels_pred = [1, 0, 1, 2, 3, 0]
    labels_actual = [0, 1, 0, 1, 3, 1]

    fms = metrics.fowlkes_mallows_score(labels_actual, labels_pred)

    # first way
    mlops.set_stat(ClusteringMetrics.FOWLKES_MALLOWS_SCORE, fms)

    # second way
    mlops.metrics.fowlkes_mallows_score(labels_true=labels_actual, labels_pred=labels_pred)

    # should throw error if not numeric number is provided
    with pytest.raises(MLOpsStatisticsException):
        mlops.set_stat(ClusteringMetrics.FOWLKES_MALLOWS_SCORE, [1, 2, 3])

    # should throw error if labels predicted is different length than actuals
    with pytest.raises(ValueError):
        labels_pred_missing_values = [0, 0, 0, 1]
        mlops.metrics.fowlkes_mallows_score(labels_true=labels_actual, labels_pred=labels_pred_missing_values)

    mlops.done()


def test_mlops_homogeneity_completeness_v_measure_apis():
    mlops.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    labels_pred = [1, 0, 1, 2, 3, 0]
    labels_actual = [0, 1, 0, 1, 3, 1]

    homogeneity, completeness, v_measure = metrics \
        .homogeneity_completeness_v_measure(labels_true=labels_actual, labels_pred=labels_pred)

    # first way
    mlops.set_stat(ClusteringMetrics.HOMOGENEITY_COMPLETENESS_V_MEASURE, [homogeneity, completeness, v_measure])

    # second way
    mlops.metrics.homogeneity_completeness_v_measure(labels_true=labels_actual, labels_pred=labels_pred)

    # should throw error if list is not of size three
    with pytest.raises(MLOpsStatisticsException):
        mlops.set_stat(ClusteringMetrics.HOMOGENEITY_COMPLETENESS_V_MEASURE, [1, 1])

    # should throw error if list is not of size three
    with pytest.raises(MLOpsStatisticsException):
        mlops.set_stat(ClusteringMetrics.HOMOGENEITY_COMPLETENESS_V_MEASURE, [1, 1, 1, 1])

    # should throw error if labels predicted is different length than actuals
    with pytest.raises(ValueError):
        labels_pred_missing_values = [0, 0, 0, 1]
        mlops.metrics.homogeneity_completeness_v_measure(labels_true=labels_actual,
                                                         labels_pred=labels_pred_missing_values)

    mlops.done()


def test_mlops_homogeneity_score_apis():
    mlops.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    labels_pred = [1, 0, 1, 2, 3, 0]
    labels_actual = [0, 1, 0, 1, 3, 1]

    hs = metrics.homogeneity_score(labels_actual, labels_pred)

    # first way
    mlops.set_stat(ClusteringMetrics.HOMOGENEITY_SCORE, hs)

    # second way
    mlops.metrics.homogeneity_score(labels_true=labels_actual, labels_pred=labels_pred)

    # should throw error if not numeric number is provided
    with pytest.raises(MLOpsStatisticsException):
        mlops.set_stat(ClusteringMetrics.HOMOGENEITY_SCORE, [1, 2, 3])

    # should throw error if labels predicted is different length than actuals
    with pytest.raises(ValueError):
        labels_pred_missing_values = [0, 0, 0, 1]
        mlops.metrics.homogeneity_score(labels_true=labels_actual, labels_pred=labels_pred_missing_values)

    mlops.done()


def test_mlops_mutual_info_score_apis():
    mlops.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    labels_pred = [1, 0, 1, 2, 3, 0]
    labels_actual = [0, 1, 0, 1, 3, 1]

    mis = metrics.mutual_info_score(labels_actual, labels_pred)

    # first way
    mlops.set_stat(ClusteringMetrics.MUTUAL_INFO_SCORE, mis)

    # second way
    mlops.metrics.mutual_info_score(labels_true=labels_actual, labels_pred=labels_pred)

    # should throw error if not numeric number is provided
    with pytest.raises(MLOpsStatisticsException):
        mlops.set_stat(ClusteringMetrics.MUTUAL_INFO_SCORE, [1, 2, 3])

    # should throw error if labels predicted is different length than actuals
    with pytest.raises(ValueError):
        labels_pred_missing_values = [0, 0, 0, 1]
        mlops.metrics.mutual_info_score(labels_true=labels_actual, labels_pred=labels_pred_missing_values)

    mlops.done()
