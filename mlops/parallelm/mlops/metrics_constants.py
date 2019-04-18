from enum import Enum


class ClassificationMetrics(Enum):
    """
    Class will hold predefined naming of all classification ML Metrics supported by ParallelM.
    """
    ACCURACY_SCORE = "Accuracy Score"
    AUC = "AUC(Area Under the Curve)"
    AVERAGE_PRECISION_SCORE = "Average Precision Score"
    BALANCED_ACCURACY_SCORE = "Balanced Accuracy Score"
    BRIER_SCORE_LOSS = "Brier Score Loss"
    CONFUSION_MATRIX = "Confusion Matrix"


class RegressionMetrics(Enum):
    """
    Class will hold predefined naming of all regression ML Metrics supported by ParallelM.
    """
    EXPLAINED_VARIANCE_SCORE = "Explained Variance Score"
    MEAN_ABSOLUTE_ERROR = "Mean Absolute Error"
    MEAN_SQUARED_ERROR = "Mean Squared Error"
    MEAN_SQUARED_LOG_ERROR = "Mean Squared Log Error"
    MEDIAN_ABSOLUTE_ERROR = "Median Absolute Error"
    R2_SCORE = "R2 Score"
