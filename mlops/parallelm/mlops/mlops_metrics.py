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
    >>> label_pos_class_prob = [0.8, 0.2, 0.9] # probabilities of positive class classification

    >>> mlops.metrics.accuracy_score(y_true=labels, y_pred=labels_pred)

    >>> fpr, tpr, thresholds = sklearn.metrics.roc_curve(labels, labels_pred, pos_label=1)

    >>> mlops.metrics.auc(x=fpr, y=tpr)

    >>> mlops.metrics.average_precision_score(y_true=labels_pred, y_score=labels_decision_values)

    >>> mlops.metrics.balanced_accuracy_score(y_true=labels, y_pred=labels_pred)

    >>> mlops.metrics.brier_score_loss(y_true=labels, y_prob=label_pos_class_prob, pos_label=1)

    >>> mlops.metrics.classification_report(labels, labels_pred)

    >>> mlops.metrics.cohen_kappa_score(labels, labels_pred)

    >>> mlops.metrics.confusion_matrix(y_true=labels, y_pred=labels_pred, labels=labels_ordered)

    >>> mlops.metrics.f1_score(labels, labels_pred, pos_label=1)

    >>> mlops.metrics.fbeta_score(labels, labels_pred, pos_label=1, beta=0.5)

    >>> mlops.metrics.hamming_loss(labels, labels_pred)

    For Regression
    >>> from parallelm.mlops import mlops

    >>> labels_pred = [1.0, 0.5, 2.5, 4.75, 7.0, 0.75] # prediction labels
    >>> labels_actual = [1.5, 0.75, 2.75, 4.5, 7.50, 0.25] # actual labels

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
        :return: cohen kappa score
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

    @staticmethod
    def f1_score(y_true, y_pred, labels=None, pos_label=1, average="binary", sample_weight=None):
        """
        Method calculates the F1 score and output it using MCenter as single value.
        In the multi-class and multi-label case, this is the weighted average of the F1 score of each class.
        :param y_true: Ground truth (correct) target values.
        :param y_pred: Estimated targets as returned by a classifier.
        :param labels: List of labels to index the matrix.
        :param pos_label: scores to report for that label only.
        :param average: Param is needed for multiclass problems. It can be any of [None, 'binary' (default), 'micro', 'macro', 'samples', 'weighted']
        :param sample_weight: Sample weights.
        :return: f1 score
        """
        # need to import only on run time.
        from parallelm.mlops import mlops as mlops
        import sklearn

        f1_score = sklearn.metrics.f1_score(y_true=y_true, y_pred=y_pred,
                                            labels=labels,
                                            pos_label=pos_label,
                                            average=average,
                                            sample_weight=sample_weight)

        mlops.set_stat(ClassificationMetrics.F1_SCORE, data=f1_score)

        return f1_score

    @staticmethod
    def fbeta_score(y_true, y_pred, beta, labels=None, pos_label=1, average="binary", sample_weight=None):
        """
        Method calculates the F-beta score and output it using MCenter as single value.
        :param y_true: Ground truth (correct) target values.
        :param y_pred: Estimated targets as returned by a classifier.
        :param beta: Weight of precision in harmonic mean.
        :param labels: List of labels to index the matrix.
        :param pos_label: scores to report for that label only.
        :param average: Param is needed for multiclass problems. It can be any of [None, 'binary' (default), 'micro', 'macro', 'samples', 'weighted']
        :param sample_weight: Sample weights.
        :return: f-beta score
        """
        from parallelm.mlops import mlops as mlops
        import sklearn

        fbeta_score = sklearn.metrics.fbeta_score(y_true=y_true, y_pred=y_pred, beta=beta,
                                                  labels=labels,
                                                  pos_label=pos_label,
                                                  average=average,
                                                  sample_weight=sample_weight)

        mlops.set_stat(ClassificationMetrics.FBETA_SCORE, data=fbeta_score)

        return fbeta_score

    @staticmethod
    def hamming_loss(y_true, y_pred, labels=None, sample_weight=None):
        """
        Method calculates the hamming loss and output it using MCenter as single value.
        :param y_true: Ground truth (correct) target values.
        :param y_pred: Estimated targets as returned by a classifier.
        :param labels: List of labels to index the matrix.
        :param sample_weight: Sample weights.
        :return: hamming loss
        """
        from parallelm.mlops import mlops as mlops
        import sklearn

        hamming_loss = sklearn.metrics.hamming_loss(y_true=y_true, y_pred=y_pred,
                                                    labels=labels,
                                                    sample_weight=sample_weight)

        mlops.set_stat(ClassificationMetrics.HAMMING_LOSS, data=hamming_loss)

        return hamming_loss

    @staticmethod
    def hinge_loss(y_true, pred_decision, labels=None, sample_weight=None):
        """
        Method calculates the hamming loss and output it using MCenter as single value.
        :param y_true: Ground truth (correct) target values.
        :param pred_decision: Predicted decisions, as output by decision_function (floats).
        :param labels: List of labels to index the matrix.
        :param sample_weight: Sample weights.
        :return: hinge loss
        """
        from parallelm.mlops import mlops as mlops
        import sklearn

        hinge_loss = sklearn.metrics.hinge_loss(y_true=y_true,
                                                pred_decision=pred_decision,
                                                labels=labels,
                                                sample_weight=sample_weight)

        mlops.set_stat(ClassificationMetrics.HINGE_LOSS, data=hinge_loss)

        return hinge_loss

    @staticmethod
    def jaccard_similarity_score(y_true, y_pred, normalize=True, sample_weight=None):
        """
        Method calculates the Jaccard similarity score and output it using MCenter as single value.
        :param y_true: Ground truth (correct) target values.
        :param y_pred: Estimated targets as returned by a classifier.
        :param normalize: If False, return the sum of the Jaccard similarity coefficient over the sample set. Otherwise, return the average.
        :param sample_weight: Sample weights.
        :return: Jaccard similarity score
        """
        from parallelm.mlops import mlops as mlops
        import sklearn

        jaccard_similarity_score = sklearn.metrics.jaccard_similarity_score(y_true=y_true, y_pred=y_pred,
                                                                            normalize=normalize,
                                                                            sample_weight=sample_weight)

        mlops.set_stat(ClassificationMetrics.JACCARD_SIMILARITY_SCORE, data=jaccard_similarity_score)

        return jaccard_similarity_score

    @staticmethod
    def log_loss(y_true, y_pred, eps=1e-15, normalize=True, sample_weight=None, labels=None):
        """
        Method calculates the log loss and output it using MCenter as single value.
        :param y_true: Ground truth (correct) target values.
        :param y_pred: Predicted probabilities.
        :param eps: probabilities are clipped to max(eps, min(1 - eps, p)).
        :param normalize: If true, return the mean loss per sample. Otherwise, return the sum of the per-sample losses.
        :param sample_weight: Sample weights.
        :param labels: List of labels to index the matrix.
        :return: Log loss
        """
        from parallelm.mlops import mlops as mlops
        import sklearn

        log_loss = sklearn.metrics.log_loss(y_true=y_true, y_pred=y_pred,
                                            eps=eps,
                                            normalize=normalize,
                                            sample_weight=sample_weight,
                                            labels=labels)

        mlops.set_stat(ClassificationMetrics.LOG_LOSS, data=log_loss)

        return log_loss

    @staticmethod
    def matthews_corrcoef(y_true, y_pred, sample_weight=None):
        """
        Method calculates the Matthews Correlation Coefficient and output it using MCenter as single value.
        :param y_true: Ground truth (correct) target values.
        :param y_pred: Estimated targets as returned by a classifier.
        :param sample_weight: Sample weights.
        :return: Matthews Correlation Coefficient
        """
        from parallelm.mlops import mlops as mlops
        import sklearn

        mcc = sklearn.metrics.matthews_corrcoef(y_true=y_true, y_pred=y_pred, sample_weight=sample_weight)

        mlops.set_stat(ClassificationMetrics.MATTHEWS_CORRELATION_COEFFICIENT, data=mcc)

        return mcc

    @staticmethod
    def precision_recall_curve(y_true, probas_pred, pos_label=None, average="macro", sample_weight=None):
        """
        Method is responsible for calculating precision recall curve and output it using graph stat object in MCenter
        :param y_true: Ground truth (correct) target values.
        :param probas_pred: Estimated probabilities or decision function.
        :param pos_label: The label of the positive class.
        :param average: This determines the type of averaging performed for calculating precision. It could be any from [None, 'micro', 'macro' (default), 'samples', 'weighted']
        :param sample_weight: Sample weights.
        :return: precision, recall, thresholds
        """
        from parallelm.mlops import mlops as mlops
        import sklearn
        import numpy as np

        precision, recall, thresholds = sklearn.metrics.precision_recall_curve(y_true=y_true,
                                                                               probas_pred=probas_pred,
                                                                               pos_label=pos_label,
                                                                               sample_weight=sample_weight)
        classes = len(np.unique(y_true))

        average_precision = sklearn.metrics.average_precision_score(y_true=y_true,
                                                                    y_score=probas_pred,
                                                                    average=average,
                                                                    sample_weight=sample_weight)

        graph_legend = "{}-class Precision Recall Curve - Average Precision: {}".format(classes, average_precision)

        mlops.set_stat(ClassificationMetrics.PRECISION_RECALL_CURVE, data=[precision, recall], legend=graph_legend)

        return precision, recall, thresholds

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
