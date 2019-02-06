
from parallelm.protobuf.ReflexEvent_pb2 import ReflexEvent
from parallelm.mlops.events.event import Event


class SystemAlert(Event):
    def __init__(self, label, description=None, data=None):
        super(SystemAlert, self).__init__(label=label, description=description, data=data,
                                          event_type=ReflexEvent.GenericSystemAlert, is_alert=True)
