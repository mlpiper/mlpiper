from enum import Enum


class ClassificationMetrics(Enum):
    """
    Class will hold predefined naming of all classification ML Metrics supported by ParallelM.
    When adding a new enum, register a stat creation function to ClassificationStatObjectFactory.registry_name_to_function.
    """

    ACCURACY_SCORE = "Accuracy Score"
    AUC = "AUC(Area Under the Curve)"
    AVERAGE_PRECISION_SCORE = "Average Precision Score"
    BALANCED_ACCURACY_SCORE = "Balanced Accuracy Score"
    BRIER_SCORE_LOSS = "Brier Score Loss"
    CLASSIFICATION_REPORT = "Classification Report"
    COHEN_KAPPA_SCORE = "Cohen Kappa Score"
    CONFUSION_MATRIX = "Confusion Matrix"
    F1_SCORE = "F1 Score"
    FBETA_SCORE = "F-beta Score"
    HAMMING_LOSS = "Hamming Loss"
    HINGE_LOSS = "Hinge Loss"
    JACCARD_SIMILARITY_SCORE = "Jaccard Similarity Score"
    LOG_LOSS = "Log Loss"


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


def check_classification_registry():
    """
    Method checks if ClassificationMetrics is registered in ClassificationStatObjectFactory registry
    """
    from parallelm.mlops.ml_metrics_stat.classification.classification_stat_object_factory \
        import ClassificationStatObjectFactory

    for each_enum in ClassificationMetrics:
        assert each_enum in ClassificationStatObjectFactory.registry_name_to_function, \
            "please register {}'s method in ClassificationStatObjectFactory.registry_name_to_function" \
                .format(each_enum)

    pass


def check_regression_registry():
    """
    Method checks if RegressionMetrics is registered in RegressionStatObjectFactory registry
    """
    from parallelm.mlops.ml_metrics_stat.regression.regression_stat_object_factory \
        import RegressionStatObjectFactory

    for each_enum in RegressionMetrics:
        assert each_enum in RegressionStatObjectFactory.registry_name_to_function, \
            "please register {}'s method in RegressionStatObjectFactory.registry_name_to_function" \
                .format(each_enum)

    pass


# this functions will run during compile time to make sure all enums are registered in appropriate registry or not
check_classification_registry()
check_regression_registry()
