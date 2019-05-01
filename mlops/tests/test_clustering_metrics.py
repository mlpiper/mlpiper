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
