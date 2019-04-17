from enum import Enum


class ClassificationMetrics(Enum):
    """
    Class will hold predefined naming of all classification ML Metrics supported by ParallelM.
    """
    CONFUSION_MATRIX = "Confusion Matrix"
    ACCURACY_SCORE = "Accuracy Score"
