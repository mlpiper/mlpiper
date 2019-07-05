from parallelm.protobuf.ReflexEvent_pb2 import ReflexEvent
from parallelm.mlops.events.event import Event


class ModelAccepted(Event):
    def __init__(self, label, model_id):
        super(ModelAccepted, self).__init__(label=label, description=None, data=None,
                                            event_type=ReflexEvent.ModelAccepted, is_alert=False, model_id=model_id)
