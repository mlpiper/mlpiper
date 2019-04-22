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

    >>> mlops.metrics.mean_absolute_error(y_true=labels, y_pred=labels_pred)

    >>> mlops.metrics.mean_squared_error(y_true=labels, y_pred=labels_pred)

    >>> mlops.metrics.mean_squared_log_error(y_true=labels, y_pred=labels_pred)

    >>> mlops.metrics.median_absolute_error(y_true=labels, y_pred=labels_pred)

    >>> mlops.metrics.r2_score(y_true=labels, y_pred=labels_pred)
    """

    ##################################################################
    ###################### classification stats ######################
    ##################################################################
    @staticmethod
    def accuracy_score(y_true, y_pred, normalize=True, sample_weight=None):
        """
        Method calculates accuracy and output it using MCenter.

        :param y_true: Ground truth (correct) labels.
        :param y_pred: Predicted labels, as returned by a classifier.
        :param normalize: If False, return the number of correctly classified samples. Otherwise, return the fraction of correctly classified samples.
        :param sample_weight: weight of samples
        :return: accuracy_score
        """

        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        accuracy_score = sklearn.metrics.accuracy_score(y_true=y_true, y_pred=y_pred, normalize=normalize,
                                                        sample_weight=sample_weight)

        mlops.set_stat(ClassificationMetrics.ACCURACY_SCORE, data=accuracy_score)

        return accuracy_score

    @staticmethod
    def auc(x, y, reorder="deprecated"):
        """
        Method calculates auc and output it using MCenter.

        :param x: x coordinates. These must be either monotonic increasing or monotonic decreasing.
        :param y: y coordinates.
        :param reorder: Whether to sort x before computing.
        :return: auc score
        """
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        auc = sklearn.metrics.auc(x=x, y=y, reorder=reorder)

        mlops.set_stat(ClassificationMetrics.AUC, data=auc)

        return auc

    @staticmethod
    def average_precision_score(y_true, y_score, average="macro", sample_weight=None):
        """
        Method calculates average precision value and output it using MCenter.

        :param y_true: True binary labels or binary label indicators.
        :param y_score: Target scores, can either be probability estimates of the positive class, confidence values, or non-thresholded measure of decisions (as returned by "decision_function" on some classifiers).
        :param average: If None, the scores for each class are returned. It can be "micro", "macro", "weighted" or "samples"
        :param sample_weight: Sample weights.
        :return: average precision score
        """
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        aps = sklearn.metrics.average_precision_score(y_true=y_true,
                                                      y_score=y_score,
                                                      average=average,
                                                      sample_weight=sample_weight)

        mlops.set_stat(ClassificationMetrics.AVERAGE_PRECISION_SCORE, data=aps)

        return aps

    @staticmethod
    def balanced_accuracy_score(y_true, y_pred, sample_weight=None, adjusted=False):
        """
        Method calculates balanced accuracy and output it using MCenter.

        :param y_true: Ground truth (correct) target values.
        :param y_pred: Estimated targets as returned by a classifier.
        :param sample_weight: Sample weights.
        :param adjusted: When true, the result is adjusted for chance, so that random performance would score 0, and perfect performance scores 1.
        :return: balanced accuracy score
        """
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        bas = sklearn.metrics.balanced_accuracy_score(y_true,
                                                      y_pred,
                                                      sample_weight=sample_weight,
                                                      adjusted=adjusted)

        mlops.set_stat(ClassificationMetrics.BALANCED_ACCURACY_SCORE, data=bas)

        return bas

    @staticmethod
    def brier_score_loss(y_true, y_prob, sample_weight=None, pos_label=None):
        """
        Method calculates brier score loss and output it using MCenter.

        :param y_true: True targets.
        :param y_prob: Probabilities of the positive class.
        :param sample_weight: Sample weights.
        :param pos_label: Label of the positive class. If None, the maximum label is used as positive class.
        :return: brier score
        """
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        bsl = sklearn.metrics.brier_score_loss(y_true,
                                               y_prob,
                                               sample_weight=sample_weight,
                                               pos_label=pos_label)

        mlops.set_stat(ClassificationMetrics.BRIER_SCORE_LOSS, data=bsl)

        return bsl

    @staticmethod
    def classification_report(y_true, y_pred,
                              labels=None, target_names=None,
                              sample_weight=None,
                              digits=2):
        """
        Method generates classification report and output it using MCenter as table.

        :param y_true: Ground truth (correct) target values.
        :param y_pred: Estimated targets as returned by a classifier.
        :param labels: List of labels to index the matrix.
        :param target_names: List of string for display names matching the labels (same order).
        :param sample_weight: Sample weights.
        :param digits: Number of digits for formatting output floating point values.
        :return: classification report string
        """
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        cr = sklearn.metrics.classification_report(y_true=y_true, y_pred=y_pred,
                                                   labels=labels, target_names=target_names,
                                                   sample_weight=sample_weight,
                                                   digits=digits)

        mlops.set_stat(ClassificationMetrics.CLASSIFICATION_REPORT, data=cr)

        return cr

    @staticmethod
    def cohen_kappa_score(y1, y2, labels=None, weights=None, sample_weight=None):
        """
        Method calculates cohen kappa score for two y1 and y2 distributions and output it using MCenter.
        :param y1: Labels assigned by the first annotator.
        :param y2: Labels assigned by the second annotator.
        :param labels: List of labels to index the matrix. This may be used to select a subset of labels. It can be None,
        :param weights: List of weighting type to calculate the score. None means no weighted, "linear", "quadratic".
        :param sample_weight: Sample weights.
        :return:
        """

        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        cks = sklearn.metrics.cohen_kappa_score(y1=y1,
                                                y2=y2,
                                                labels=labels,
                                                weights=weights,
                                                sample_weight=sample_weight)

        mlops.set_stat(ClassificationMetrics.COHEN_KAPPA_SCORE, data=cks)

        return cks

    @staticmethod
    def confusion_matrix(y_true, y_pred, labels, sample_weight=None):
        """
        Method calculates confusion matrix and output it using MCenter as table.

        :param y_true: Ground truth (correct) target values.
        :param y_pred: Estimated targets as returned by a classifier.
        :param labels: List of labels to index the matrix.
        :param sample_weight: Sample weights.
        :return: confusion matrix
        """
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        cm = sklearn.metrics.confusion_matrix(y_true, y_pred, labels, sample_weight)

        mlops.set_stat(ClassificationMetrics.CONFUSION_MATRIX, data=cm, labels=labels)

        return cm

    ##################################################################
    ######################## regression stats ########################
    ##################################################################

    @staticmethod
    def explained_variance_score(y_true, y_pred, sample_weight=None, multioutput="uniform_average"):
        """
        Method calculates explained variance score and output it using MCenter.

        :param y_true: Ground truth (correct) target values.
        :param y_pred: Estimated target values.
        :param sample_weight: Sample weights.
        :param multioutput: Defines aggregating of multiple output scores. It can be "raw_values", "uniform_average", "variance_weighted"
        :return: explained variance score
        """
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        evs = sklearn.metrics.explained_variance_score(y_true=y_true,
                                                       y_pred=y_pred,
                                                       sample_weight=sample_weight,
                                                       multioutput=multioutput)

        mlops.set_stat(RegressionMetrics.EXPLAINED_VARIANCE_SCORE, data=evs)

        return evs

    @staticmethod
    def mean_absolute_error(y_true, y_pred, sample_weight=None, multioutput="uniform_average"):
        """
        Method calculates mean absolute error and output it using MCenter.

        :param y_true: Ground truth (correct) target values.
        :param y_pred: Estimated target values.
        :param sample_weight: Sample weights.
        :param multioutput: Defines aggregating of multiple output scores. It can be "raw_values", "uniform_average"
        :return: mean absolute error
        """
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        mae = sklearn.metrics.mean_absolute_error(y_true=y_true,
                                                  y_pred=y_pred,
                                                  sample_weight=sample_weight,
                                                  multioutput=multioutput)

        mlops.set_stat(RegressionMetrics.MEAN_ABSOLUTE_ERROR, data=mae)

        return mae

    @staticmethod
    def mean_squared_error(y_true, y_pred, sample_weight=None, multioutput="uniform_average"):
        """
        Method calculates mean squared error and output it using MCenter.

        :param y_true: Ground truth (correct) target values.
        :param y_pred: Estimated target values.
        :param sample_weight: Sample weights.
        :param multioutput: Defines aggregating of multiple output scores. It can be "raw_values", "uniform_average"
        :return: mean squared error
        """
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        mse = sklearn.metrics.mean_squared_error(y_true=y_true,
                                                 y_pred=y_pred,
                                                 sample_weight=sample_weight,
                                                 multioutput=multioutput)

        mlops.set_stat(RegressionMetrics.MEAN_SQUARED_ERROR, data=mse)

        return mse

    @staticmethod
    def mean_squared_log_error(y_true, y_pred, sample_weight=None, multioutput="uniform_average"):
        """
        Method calculates mean squared log error and output it using MCenter.

        :param y_true: Ground truth (correct) target values.
        :param y_pred: Estimated target values.
        :param sample_weight: Sample weights.
        :param multioutput: Defines aggregating of multiple output scores. It can be "raw_values", "uniform_average"
        :return: mean squared log error
        """
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        msle = sklearn.metrics.mean_squared_log_error(y_true=y_true,
                                                      y_pred=y_pred,
                                                      sample_weight=sample_weight,
                                                      multioutput=multioutput)

        mlops.set_stat(RegressionMetrics.MEAN_SQUARED_LOG_ERROR, data=msle)

        return msle

    @staticmethod
    def median_absolute_error(y_true, y_pred):
        """
        Method calculates median absolute error and output it using MCenter.

        :param y_true: Ground truth (correct) target values.
        :param y_pred: Estimated target values.

        :return: median absolute error
        """
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        mae = sklearn.metrics.median_absolute_error(y_true=y_true,
                                                    y_pred=y_pred)

        mlops.set_stat(RegressionMetrics.MEDIAN_ABSOLUTE_ERROR, data=mae)

        return mae

    @staticmethod
    def r2_score(y_true, y_pred, sample_weight=None, multioutput="uniform_average"):
        """
        Method calculates r2 score and output it using MCenter.

        :param y_true: Ground truth (correct) target values.
        :param y_pred: Estimated target values.
        :param sample_weight: Sample weights.
        :param multioutput: Defines aggregating of multiple output scores. It can be "raw_values", "uniform_average", "variance_weighted"
        :return: r2 score
        """

        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        r2_score = sklearn.metrics.r2_score(y_true=y_true,
                                            y_pred=y_pred,
                                            sample_weight=sample_weight,
                                            multioutput=multioutput)

        mlops.set_stat(RegressionMetrics.R2_SCORE, data=r2_score)

        return r2_score
