from parallelm.mlops.metrics_constants import ClassificationMetrics, RegressionMetrics
from parallelm.mlops.singelton import Singleton


@Singleton
class MLOpsMetrics(object):
    """
    Class is responsible for giving user sklearn alike code representation for using ParallelM's mlops apis.
    Class will support classification, regression and clustering stats.
    :Example:
    For Classification

    >>> from parallelm.mlops import mlops

    >>> # Output ML Stat - For Example Confusion Matrix as Table
    >>> labels_pred = [1, 0, 1] # prediction labels
    >>> labels = [0, 1, 0] # actual labels
    >>> labels_ordered = [0, 1] # order of labels to use for creating confusion matrix.
    >>> labels_decision_values = [0.9, 0.85, 0.9] # distance from hyper plane

    >>> mlops.metrics.accuracy_score(y_true=labels, y_pred=labels_pred)

    >>> fpr, tpr, thresholds = sklearn.metrics.roc_curve(labels, labels_pred, pos_label=2)

    >>> mlops.metrics.auc(x=fpr, y=tpr)

    >>> pm.metrics.average_precision_score(y_true=labels_pred, y_score=labels_decision_values)

    >>> mlops.metrics.confusion_matrix(y_true=labels, y_pred=labels_pred, labels=labels_ordered)


    For Regression
    >>> from parallelm.mlops import mlops

    >>> labels_pred = [1.0, 0.5, 2.5, 4.75, 7.0, 0.75]
    >>> labels_actual = [1.5, 0.75, 2.75, 4.5, 7.50, 0.25]

    >>> mlops.metrics.explained_variance_score(y_true=labels_actual, y_pred=labels_pred)

    """

    ##################################################################
    ###################### classification stats ######################
    ##################################################################
    @staticmethod
    def accuracy_score(y_true, y_pred, normalize=True, sample_weight=None):
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        accuracy_score = sklearn.metrics.accuracy_score(y_true=y_true, y_pred=y_pred, normalize=normalize,
                                                        sample_weight=sample_weight)

        mlops.set_stat(ClassificationMetrics.ACCURACY_SCORE, data=accuracy_score)

    @staticmethod
    def auc(x, y, reorder="deprecated"):
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        auc = sklearn.metrics.auc(x=x, y=y, reorder=reorder)

        mlops.set_stat(ClassificationMetrics.AUC, data=auc)

    @staticmethod
    def average_precision_score(y_true, y_score, average="macro", sample_weight=None):
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        aps = sklearn.metrics.average_precision_score(y_true=y_true,
                                                      y_score=y_score,
                                                      average=average,
                                                      sample_weight=sample_weight)

        mlops.set_stat(ClassificationMetrics.AVERAGE_PRECISION_SCORE, data=aps)

    @staticmethod
    def confusion_matrix(y_true, y_pred, labels, sample_weight=None):
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        cm = sklearn.metrics.confusion_matrix(y_true, y_pred, labels, sample_weight)

        mlops.set_stat(ClassificationMetrics.CONFUSION_MATRIX, data=cm, labels=labels)

    ##################################################################
    ######################## regression stats ########################
    ##################################################################

    @staticmethod
    def explained_variance_score(y_true, y_pred, sample_weight=None, multioutput="uniform_average"):
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        evs = sklearn.metrics.explained_variance_score(y_true=y_true,
                                                       y_pred=y_pred,
                                                       sample_weight=sample_weight,
                                                       multioutput=multioutput)

        mlops.set_stat(RegressionMetrics.EXPLAINED_VARIANCE_SCORE, data=evs)

    @staticmethod
    def mean_absolute_error(y_true, y_pred, sample_weight=None, multioutput="uniform_average"):
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        mae = sklearn.metrics.mean_absolute_error(y_true=y_true,
                                                  y_pred=y_pred,
                                                  sample_weight=sample_weight,
                                                  multioutput=multioutput)

        mlops.set_stat(RegressionMetrics.MEAN_ABSOLUTE_ERROR, data=mae)

    @staticmethod
    def mean_squared_error(y_true, y_pred, sample_weight=None, multioutput="uniform_average"):
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        mse = sklearn.metrics.mean_squared_error(y_true=y_true,
                                                 y_pred=y_pred,
                                                 sample_weight=sample_weight,
                                                 multioutput=multioutput)

        mlops.set_stat(RegressionMetrics.MEAN_SQUARED_ERROR, data=mse)
