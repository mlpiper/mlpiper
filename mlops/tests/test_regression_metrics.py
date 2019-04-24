import pytest
import sklearn

from parallelm.mlops import mlops as pm
from parallelm.mlops.metrics_constants import RegressionMetrics
from parallelm.mlops.mlops_exception import MLOpsStatisticsException
from parallelm.mlops.mlops_mode import MLOpsMode


def test_mlops_explained_variance_score_apis():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    labels_pred = [1.0, 0.5, 2.5, 4.75, 7.0, 0.75]
    labels_actual = [1.5, 0.75, 2.75, 4.5, 7.50, 0.25]

    evs = sklearn.metrics.explained_variance_score(labels_actual, labels_pred)

    # first way
    pm.set_stat(RegressionMetrics.EXPLAINED_VARIANCE_SCORE, evs)

    # array list is allowed as well
    pm.set_stat(RegressionMetrics.EXPLAINED_VARIANCE_SCORE, [1, 2, 3])

    # second way
    pm.metrics.explained_variance_score(y_true=labels_actual, y_pred=labels_pred)

    # should throw error if labels predicted is different length than actuals
    with pytest.raises(ValueError):
        labels_pred_missing_values = [1.0, 0.5, 7.0, 0.75]
        pm.metrics.explained_variance_score(y_true=labels_actual, y_pred=labels_pred_missing_values)

    sample_weight = [0.9, 0.1, 0.5, 0.9, 1.0, 0]

    # testing with sample weights as well
    pm.metrics.explained_variance_score(y_true=labels_actual,
                                        y_pred=labels_pred,
                                        sample_weight=sample_weight)

    labels_2d_actual = [[1.0, 0.5], [2.5, 4.75], [7.0, 0.75]]
    labels_2d_pred = [[1.5, 0.75], [2.75, 4.5], [7.50, 0.25]]

    # testing where result will be multiple float values
    evs = pm.metrics.explained_variance_score(y_true=labels_2d_actual,
                                              y_pred=labels_2d_pred,
                                              multioutput="raw_values")

    assert len(evs) == 2

    pm.done()


def test_mlops_mean_absolute_error_apis():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    labels_pred = [1.0, 0.5, 2.5, 4.75, 7.0, 0.75]
    labels_actual = [1.5, 0.75, 2.75, 4.5, 7.50, 0.25]

    mae = sklearn.metrics.mean_absolute_error(labels_actual, labels_pred)

    # first way
    pm.set_stat(RegressionMetrics.MEAN_ABSOLUTE_ERROR, mae)

    # array list is allowed as well
    pm.set_stat(RegressionMetrics.MEAN_ABSOLUTE_ERROR, [1, 2, 3])

    # second way
    pm.metrics.mean_absolute_error(y_true=labels_actual, y_pred=labels_pred)

    # should throw error if labels predicted is different length than actuals
    with pytest.raises(ValueError):
        labels_pred_missing_values = [1.0, 0.5, 7.0, 0.75]
        pm.metrics.mean_absolute_error(y_true=labels_actual, y_pred=labels_pred_missing_values)

    sample_weight = [0.9, 0.1, 0.5, 0.9, 1.0, 0]

    # testing with sample weights as well
    pm.metrics.mean_absolute_error(y_true=labels_actual,
                                   y_pred=labels_pred,
                                   sample_weight=sample_weight)

    labels_2d_actual = [[1.0, 0.5], [2.5, 4.75], [7.0, 0.75]]
    labels_2d_pred = [[1.5, 0.75], [2.75, 4.5], [7.50, 0.25]]

    mae = pm.metrics.mean_absolute_error(y_true=labels_2d_actual,
                                         y_pred=labels_2d_pred,
                                         multioutput="raw_values")

    assert len(mae) == 2

    pm.done()


def test_mlops_mean_squared_error_apis():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    labels_pred = [1.0, 0.5, 2.5, 4.75, 7.0, 0.75]
    labels_actual = [1.5, 0.75, 2.75, 4.5, 7.50, 0.25]

    mse = sklearn.metrics.mean_squared_error(labels_actual, labels_pred)

    # first way
    pm.set_stat(RegressionMetrics.MEAN_SQUARED_ERROR, mse)

    # second way
    pm.metrics.mean_squared_error(y_true=labels_actual, y_pred=labels_pred)

    # array list is allowed as well
    pm.set_stat(RegressionMetrics.MEAN_SQUARED_ERROR, [1, 2, 3])

    # should throw error if labels predicted is different length than actuals
    with pytest.raises(ValueError):
        labels_pred_missing_values = [1.0, 0.5, 7.0, 0.75]
        pm.metrics.mean_squared_error(y_true=labels_actual, y_pred=labels_pred_missing_values)

    sample_weight = [0.9, 0.1, 0.5, 0.9, 1.0, 0]

    # testing with sample weights as well
    pm.metrics.mean_squared_error(y_true=labels_actual,
                                  y_pred=labels_pred,
                                  sample_weight=sample_weight)

    labels_2d_actual = [[1.0, 0.5], [2.5, 4.75], [7.0, 0.75]]
    labels_2d_pred = [[1.5, 0.75], [2.75, 4.5], [7.50, 0.25]]

    mse = pm.metrics.mean_squared_error(y_true=labels_2d_actual,
                                        y_pred=labels_2d_pred,
                                        multioutput="raw_values")

    assert len(mse) == 2

    pm.done()


def test_mlops_mean_squared_log_error_apis():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    labels_pred = [1.0, 0.5, 2.5, 4.75, 7.0, 0.75]
    labels_actual = [1.5, 0.75, 2.75, 4.5, 7.50, 0.25]

    msle = sklearn.metrics.mean_squared_log_error(labels_actual, labels_pred)

    # first way
    pm.set_stat(RegressionMetrics.MEAN_SQUARED_LOG_ERROR, msle)
    pm.set_stat(RegressionMetrics.MEAN_SQUARED_LOG_ERROR, [1, 2, 3])

    # second way
    pm.metrics.mean_squared_log_error(y_true=labels_actual, y_pred=labels_pred)

    # should throw error if labels predicted is different length than actuals
    with pytest.raises(ValueError):
        labels_pred_missing_values = [1.0, 0.5, 7.0, 0.75]
        pm.metrics.mean_squared_log_error(y_true=labels_actual, y_pred=labels_pred_missing_values)

    # should throw error if labels contain negative values
    with pytest.raises(ValueError):
        labels_pred_neg = [1.0, -0.5, 2.5, 4.75, 7.0, 0.75]
        labels_actual_neg = [1.5, -0.75, 2.75, 4.5, 7.50, 0.25]

        pm.metrics.mean_squared_log_error(y_true=labels_actual_neg, y_pred=labels_pred_neg)

    sample_weight = [0.9, 0.1, 0.5, 0.9, 1.0, 0]

    # testing with sample weights as well
    pm.metrics.mean_squared_log_error(y_true=labels_actual,
                                      y_pred=labels_pred,
                                      sample_weight=sample_weight)

    labels_2d_actual = [[1.0, 0.5], [2.5, 4.75], [7.0, 0.75]]
    labels_2d_pred = [[1.5, 0.75], [2.75, 4.5], [7.50, 0.25]]

    msle = pm.metrics.mean_squared_log_error(y_true=labels_2d_actual,
                                             y_pred=labels_2d_pred,
                                             multioutput="raw_values")

    assert len(msle) == 2

    pm.done()


def test_mlops_median_absolute_error_apis():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    labels_pred = [1.0, 0.5, 2.5, 4.75, 7.0, 0.75]
    labels_actual = [1.5, 0.75, 2.75, 4.5, 7.50, 0.25]

    mae = sklearn.metrics.median_absolute_error(labels_actual, labels_pred)

    # first way
    pm.set_stat(RegressionMetrics.MEDIAN_ABSOLUTE_ERROR, mae)

    # second way
    pm.metrics.median_absolute_error(y_true=labels_actual, y_pred=labels_pred)

    # should throw error if not numeric number is provided
    with pytest.raises(MLOpsStatisticsException):
        pm.set_stat(RegressionMetrics.MEDIAN_ABSOLUTE_ERROR, [1, 2, 3])

    # should throw error if labels predicted is different length than actuals
    with pytest.raises(ValueError):
        labels_pred_missing_values = [1.0, 0.5, 7.0, 0.75]
        pm.metrics.mean_absolute_error(y_true=labels_actual, y_pred=labels_pred_missing_values)

    pm.done()


def test_mlops_r2_score_apis():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    labels_pred = [1.0, 0.5, 2.5, 4.75, 7.0, 0.75]
    labels_actual = [1.5, 0.75, 2.75, 4.5, 7.50, 0.25]

    mae = sklearn.metrics.r2_score(labels_actual, labels_pred)

    # first way
    pm.set_stat(RegressionMetrics.R2_SCORE, mae)

    # second way
    pm.metrics.r2_score(y_true=labels_actual, y_pred=labels_pred)

    # should throw error if not numeric number is provided
    with pytest.raises(MLOpsStatisticsException):
        pm.set_stat(RegressionMetrics.R2_SCORE, [1, 2, 3])

    # should throw error if labels predicted is different length than actuals
    with pytest.raises(ValueError):
        labels_pred_missing_values = [1.0, 0.5, 7.0, 0.75]
        pm.metrics.r2_score(y_true=labels_actual, y_pred=labels_pred_missing_values)

    sample_weight = [0.9, 0.1, 0.5, 0.9, 1.0, 0]

    # testing with sample weights as well
    pm.metrics.r2_score(y_true=labels_actual,
                        y_pred=labels_pred,
                        sample_weight=sample_weight)

    pm.done()
