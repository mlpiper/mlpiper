from parallelm.mlops.metrics_constants import ClassificationMetrics
from parallelm.mlops.singelton import Singleton


@Singleton
class MLOpsMetrics(object):
    """
    Class is responsible for giving user sklearn alike code representation for using ParallelM's mlops apis.
    Class will support classification, regression and clustering stats.
    :Example:

    >>> from parallelm.mlops import mlops

    >>> # Output ML Stat - For Example Confusion Matrix as Table
    >>> labels_pred = [1, 0 , 1] # prediction labels
    >>> labels = [0, 1, 0] # actual labels
    >>> labels_ordered = [0, 1] # order of labels to use for creating confusion matrix.

    >>> mlops.metrics.confusion_matrix(y_true=labels, y_pred=labels_pred, labels=labels_ordered)
    """

    # classification stats
    @staticmethod
    def confusion_matrix(y_true, y_pred, labels, sample_weight=None):
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        cm = sklearn.metrics.confusion_matrix(y_true, y_pred, labels, sample_weight)

        mlops.set_stat(ClassificationMetrics.CONFUSION_MATRIX, cm, labels=labels)
