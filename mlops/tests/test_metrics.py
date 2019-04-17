import pytest
from sklearn import metrics

from parallelm.mlops import mlops as pm
from parallelm.mlops.metrics_constants import ClassificationMetrics
from parallelm.mlops.mlops_exception import MLOpsStatisticsException
from parallelm.mlops.mlops_mode import MLOpsMode


def test_mlops_accuracy_score_apis():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    labels_pred = [1, 0, 1, 1, 1, 0]
    labels_actual = [0, 1, 0, 0, 0, 1]

    accuracy_score = metrics.accuracy_score(labels_actual, labels_pred)

    # first way
    pm.set_stat(ClassificationMetrics.ACCURACY_SCORE, accuracy_score)

    # second way
    pm.metrics.accuracy_score(y_true=labels_actual, y_pred=labels_pred)

    # should throw error if not numeric number is provided
    with pytest.raises(MLOpsStatisticsException):
        pm.set_stat(ClassificationMetrics.ACCURACY_SCORE, [1, 2, 3])

    # should throw error if labels predicted is different length than actuals
    with pytest.raises(ValueError):
        labels_pred_missing_values = [0, 0, 0, 1]
        pm.metrics.accuracy_score(y_true=labels_actual, y_pred=labels_pred_missing_values)

    sample_weight = [0.9, 0.1, 0.5, 0.9, 1.0, 0]

    # testing with sample weights as well
    pm.metrics.accuracy_score(y_true=labels_actual,
                              y_pred=labels_pred,
                              sample_weight=sample_weight)

    pm.done()


def test_mlops_confusion_metrics_apis():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    labels_pred = [1, 0, 1, 1, 1, 0]
    labels_actual = [0, 1, 0, 0, 0, 1]
    labels_ordered = [0, 1]

    cm = metrics.confusion_matrix(labels_actual, labels_pred, labels=labels_ordered)

    # first way
    pm.set_stat(ClassificationMetrics.CONFUSION_MATRIX, cm, labels=labels_ordered)

    # second way
    pm.metrics.confusion_matrix(y_true=labels_actual, y_pred=labels_pred, labels=labels_ordered)

    # should throw error if labels are not provided
    with pytest.raises(MLOpsStatisticsException):
        pm.set_stat(ClassificationMetrics.CONFUSION_MATRIX, cm)

    with pytest.raises(MLOpsStatisticsException):
        pm.metrics.confusion_matrix(y_true=labels_actual, y_pred=labels_pred, labels=None)

    # should throw error if labels predicted is different length than actuals
    with pytest.raises(ValueError):
        labels_pred_missing_values = [0, 0, 0, 1]
        pm.metrics.confusion_matrix(y_true=labels_actual, y_pred=labels_pred_missing_values, labels=None)

    sample_weight = [0.9, 0.1, 0.5, 0.9, 1.0, 0]

    # testing with sample weights as well
    pm.metrics.confusion_matrix(y_true=labels_actual,
                                y_pred=labels_pred,
                                labels=labels_ordered,
                                sample_weight=sample_weight)

    pm.done()
