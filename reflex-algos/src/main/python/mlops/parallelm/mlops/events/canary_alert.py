import json

from parallelm.protobuf.ReflexEvent_pb2 import ReflexEvent
from parallelm.mlops.events.event import Event


class CanaryAlert(Event):
    """
    Sends a canary alert to the ParallelM service.
    Canary pipelines and used to compare prediction results from one pipeline to that of another.
    The predictions of the pipelines is compared to produce an overlap score.
    An alert indicates whether the overlap score differs by more than a threshold amount.
    """
    def __init__(self, label, is_healthy=False, score=None, threshold=None):

        hist_map = {'threshold': threshold, 'attributes': 'prediction', 'scores': score}

        diverged_value = ""
        description = "Canary Health is Good"
        if is_healthy is False:
            diverged_value = "prediction"
            description = "Canary prediction distributions diverge with a threshold set to {}".format(threshold)

        hist_map['divergedAttributes'] = diverged_value

        data_map = {'type': "canaryPredictionComparison", 'description': description, 'isHealthy': is_healthy}
        data_map['histogramComparison'] = hist_map

        super(CanaryAlert, self).__init__(label=label,
                                          description=description,
                                          event_type=ReflexEvent.CanaryHealth,
                                          is_alert=True)
        self._set_data(data_map)

