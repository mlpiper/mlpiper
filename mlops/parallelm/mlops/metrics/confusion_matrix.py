import numpy as np

from parallelm.mlops.metrics_constants import ClassificationMetrics
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats.table import Table
from parallelm.protobuf.ReflexEvent_pb2 import ReflexEvent


class ConfusionMatrix(object):

    @staticmethod
    def _set_confusion_matrix(cm_nd_array, model_id, stat_object_method, **kwargs):
        labels = kwargs.get('labels', None)

        if labels is not None:
            if isinstance(cm_nd_array, np.ndarray) and isinstance(labels, list):
                if len(cm_nd_array) == len(labels):
                    # Output Confusion Matrix
                    labels_string = [str(i) for i in labels]
                    cm_matrix = Table().name(ClassificationMetrics.CONFUSION_MATRIX.value).cols(labels_string)

                    for index in range(len(cm_nd_array)):
                        cm_matrix.add_row(labels_string[index], list(cm_nd_array[index]))

                    stat_object_method(mlops_stat=cm_matrix.get_mlops_stat(model_id=model_id),
                                       reflex_event_message_type=ReflexEvent.StatsMessage)
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
