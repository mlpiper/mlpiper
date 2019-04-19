from enum import Enum


class ClassificationMetrics(Enum):
    """
    Class will hold predefined naming of all classification ML Metrics supported by ParallelM.
    WWhen adding a new enum, register a stat creation function to ClassificationStatObjectFactory.registry_name_to_function.
    """
    ACCURACY_SCORE = "Accuracy Score"
    AUC = "AUC(Area Under the Curve)"
    AVERAGE_PRECISION_SCORE = "Average Precision Score"
    BALANCED_ACCURACY_SCORE = "Balanced Accuracy Score"
    BRIER_SCORE_LOSS = "Brier Score Loss"
    CLASSIFICATION_REPORT = "Classification Report"
    CONFUSION_MATRIX = "Confusion Matrix"


class RegressionMetrics(Enum):
    """
    Class will hold predefined naming of all regression ML Metrics supported by ParallelM.
    When adding a new enum, register a stat creation function to RegressionStatObjectFactory.registry_name_to_function.
    """
    EXPLAINED_VARIANCE_SCORE = "Explained Variance Score"
    MEAN_ABSOLUTE_ERROR = "Mean Absolute Error"
    MEAN_SQUARED_ERROR = "Mean Squared Error"
    MEAN_SQUARED_LOG_ERROR = "Mean Squared Log Error"
    MEDIAN_ABSOLUTE_ERROR = "Median Absolute Error"
    R2_SCORE = "R2 Score"
