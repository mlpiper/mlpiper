import numpy as np

from parallelm.mlops.metrics_constants import ClassificationMetrics
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats.table import Table


class ConfusionMatrix(object):
    """
    Responsibility of this class is basically creating table object out of input array and list of labels.
    """

    @staticmethod
    def _get_mlops_cm_table_object(cm_nd_array, **kwargs):
        """
        Method will create MLOps Table stat object from ndarray and labels argument coming from kwargs
        :param cm_nd_array: Array representation of confusion matrix
        :param kwargs: `labels` used for representation of confusion matrix
        :return: MLOps Table object generated from array
        """
        labels = kwargs.get('labels', None)

        if labels is not None:
            if isinstance(cm_nd_array, np.ndarray) and isinstance(labels, list):
                if len(cm_nd_array) == len(labels):
                    # Output Confusion Matrix
                    labels_string = [str(i) for i in labels]
                    cm_matrix = Table().name(ClassificationMetrics.CONFUSION_MATRIX.value).cols(labels_string)

                    for index in range(len(cm_nd_array)):
                        cm_matrix.add_row(labels_string[index], list(cm_nd_array[index]))

                    return cm_matrix

                else:
                    raise MLOpsException(
                        "Size of Confusion Matrix = {} and length of labels = {} does not match".format(
                            len(cm_nd_array), len(labels)))
            else:
                raise MLOpsException(
                    "Confusion Matrix should be of type numpy nd-array and labels should be of type list")

        else:
            raise MLOpsException(
                "For outputting confusion matrix labels must be provided using extra `labels` argument to mlops apis.")

        return None
