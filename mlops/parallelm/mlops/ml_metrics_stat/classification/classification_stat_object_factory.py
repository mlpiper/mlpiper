import numpy as np
from six import string_types

from parallelm.mlops.metrics_constants import ClassificationMetrics
from parallelm.mlops.ml_metrics_stat.ml_stat_object_creator import MLStatObjectCreator
from parallelm.mlops.mlops_exception import MLOpsStatisticsException


class ClassificationStatObjectFactory(object):
    """
    Responsibility of this class is basically creating stat object for classification stat.
    """

    @staticmethod
    def get_mlops_accuracy_score_stat_object(**kwargs):
        """
        Method will create MLOps Single value stat object from numeric real number - accuracy score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: numeric value of accuracy
        :return: Single Value stat object which has accuracy score embedded inside
        """
        accuracy_score = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=ClassificationMetrics.ACCURACY_SCORE.value,
                                         single_value=accuracy_score)

        return single_value, category

    @staticmethod
    def get_mlops_auc_stat_object(**kwargs):
        """
        Method will create MLOps Single value stat object from numeric real number - auc score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: numeric value of auc
        :return: Single Value stat object which has auc score embedded inside
        """
        auc = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=ClassificationMetrics.AUC.value,
                                         single_value=auc)

        return single_value, category

    @staticmethod
    def get_mlops_average_precision_score_stat_object(**kwargs):
        """
        Method will create MLOps Single value stat object from numeric real number - average precision score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: numeric value of average precision score
        :return: Single Value stat object which has aps score embedded inside
        """
        aps = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=ClassificationMetrics.AVERAGE_PRECISION_SCORE.value,
                                         single_value=aps)

        return single_value, category

    @staticmethod
    def get_mlops_balanced_accuracy_score_stat_object(**kwargs):
        """
        Method will create MLOps Single value stat object from numeric real number - balanced accuracy score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: numeric value of balanced accuracy
        :return: Single Value stat object which has accuracy score embedded inside
        """
        balanced_accuracy_score = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=ClassificationMetrics.BALANCED_ACCURACY_SCORE.value,
                                         single_value=balanced_accuracy_score)

        return single_value, category

    @staticmethod
    def get_mlops_brier_score_loss_stat_object(**kwargs):
        """
        Method will create MLOps Single value stat object from numeric real number - brier score loss
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: numeric value of brier score loss
        :return: Single Value stat object which has brier score embedded inside
        """
        bsl = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=ClassificationMetrics.BRIER_SCORE_LOSS.value,
                                         single_value=bsl)

        return single_value, category

    @staticmethod
    def get_mlops_classification_report_stat_object(**kwargs):
        """
        Method will create MLOps table value stat object from classification report string.
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: classification report string.
        :return: Table Value stat object which has classification report embedded inside
        """
        cr = kwargs.get('data', None)

        if cr is not None:
            if isinstance(cr, string_types):
                try:
                    array_report = list()
                    for row in cr.split("\n"):
                        parsed_row = [x for x in row.split("  ") if len(x) > 0]
                        if len(parsed_row) > 0:
                            array_report.append(parsed_row)

                    first_header_should_be = ['precision', 'recall', 'f1-score', 'support']

                    table_object, category = MLStatObjectCreator \
                        .get_table_value_stat_object(name=ClassificationMetrics.CLASSIFICATION_REPORT.value,
                                                     list_2d=array_report,
                                                     match_header_pattern=first_header_should_be)

                    return table_object, category

                except Exception as e:
                    raise MLOpsStatisticsException(
                        "error happened while outputting classification report as table. Got classification string {}.\n error: {}"
                            .format(cr, e))

            else:
                raise MLOpsStatisticsException(
                    "type of classification should be of string, but received {}".format(type(cr)))
        else:
            raise MLOpsStatisticsException \
                ("cr object for outputting classification report cannot be None.")

    @staticmethod
    def get_mlops_cohen_kappa_score_stat_object(**kwargs):
        """
        Method will create MLOps Single value stat object from numeric real number - cohen kappa score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: numeric value of cohen kappa score
        :return: Single Value stat object which has cohen kappa score embedded inside
        """
        cks = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=ClassificationMetrics.COHEN_KAPPA_SCORE.value,
                                         single_value=cks)

        return single_value, category

    @staticmethod
    def get_mlops_confusion_matrix_stat_object(**kwargs):
        """
        Method will create MLOps Table stat object from ndarray and labels argument coming from kwargs (`labels`).
        It is not recommended to access this method without understanding table data structure that it is returning.
        :param kwargs: `data` - Array representation of confusion matrix & `labels` used for representation of confusion matrix
        :return: MLOps Table object generated from array
        """
        cm_nd_array = kwargs.get('data', None)
        labels = kwargs.get('labels', None)

        if labels is not None:
            if isinstance(cm_nd_array, np.ndarray) and isinstance(labels, list):
                if len(cm_nd_array) == len(labels):
                    array_report = list()

                    header = [str(i) for i in labels]
                    array_report.append(header)
                    for index in range(len(cm_nd_array)):
                        row = list(cm_nd_array[index])
                        # adding first col as class it represents
                        row.insert(0, header[index])

                        array_report.append(row)

                    table_object, category = MLStatObjectCreator \
                        .get_table_value_stat_object(name=ClassificationMetrics.CONFUSION_MATRIX.value,
                                                     list_2d=array_report)

                    return table_object, category

                else:
                    raise MLOpsStatisticsException \
                        ("Size of Confusion Matrix = {} and length of labels = {} does not match"
                         .format(len(cm_nd_array), len(labels)))
            else:
                raise MLOpsStatisticsException \
                    ("Confusion Matrix should be of type numpy nd-array and labels should be of type list")

        else:
            raise MLOpsStatisticsException \
                ("For outputting confusion matrix labels must be provided using extra `labels` argument to mlops apis.")

    @staticmethod
    def get_mlops_f1_score_stat_object(**kwargs):
        """
        Method will create MLOps Single value stat object from numeric real number - f1 score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: numeric value of f1 score
        :return: Single Value stat object which has f1 score embedded inside
        """
        f1_score = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=ClassificationMetrics.F1_SCORE.value,
                                         single_value=f1_score)

        return single_value, category

    @staticmethod
    def get_mlops_fbeta_score_stat_object(**kwargs):
        """
        Method will create MLOps Single value stat object from numeric real number - f-beta score
        It is not recommended to access this method without understanding single value data structure that it is returning.
        :param kwargs: numeric value of f-beta score
        :return: Single Value stat object which has f-beta score embedded inside
        """
        fbeta_score = kwargs.get('data', None)

        single_value, category = MLStatObjectCreator. \
            get_single_value_stat_object(name=ClassificationMetrics.FBETA_SCORE.value,
                                         single_value=fbeta_score)

        return single_value, category

    # registry holds name to function mapping. please add __func__ for making static object callable from below getter method.
    registry_name_to_function = {
        ClassificationMetrics.ACCURACY_SCORE: get_mlops_accuracy_score_stat_object.__func__,
        ClassificationMetrics.AUC: get_mlops_auc_stat_object.__func__,
        ClassificationMetrics.AVERAGE_PRECISION_SCORE: get_mlops_average_precision_score_stat_object.__func__,
        ClassificationMetrics.BALANCED_ACCURACY_SCORE: get_mlops_balanced_accuracy_score_stat_object.__func__,
        ClassificationMetrics.BRIER_SCORE_LOSS: get_mlops_brier_score_loss_stat_object.__func__,
        ClassificationMetrics.CLASSIFICATION_REPORT: get_mlops_classification_report_stat_object.__func__,
        ClassificationMetrics.COHEN_KAPPA_SCORE: get_mlops_cohen_kappa_score_stat_object.__func__,
        ClassificationMetrics.CONFUSION_MATRIX: get_mlops_confusion_matrix_stat_object.__func__,
        ClassificationMetrics.F1_SCORE: get_mlops_f1_score_stat_object.__func__,
        ClassificationMetrics.FBETA_SCORE: get_mlops_fbeta_score_stat_object.__func__
    }

    @staticmethod
    def get_stat_object(name, **kwargs):

        return ClassificationStatObjectFactory.registry_name_to_function[name](**kwargs)
