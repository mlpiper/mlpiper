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
