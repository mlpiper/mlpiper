from enum import Enum


class ClassificationMetrics(Enum):
    """
    Class will hold predefined naming of all classification ML Metrics supported by ParallelM.
    """
    ACCURACY_SCORE = "Accuracy Score"
    AUC = "AUC(Area Under the Curve)"
    CONFUSION_MATRIX = "Confusion Matrix"
