from parallelm.protobuf.ReflexEvent_pb2 import ReflexEvent
from parallelm.mlops.mlops_exception import MLOpsException
import pickle
import six

class Event(object):
    """
    This will be the user facing event class
    """

    def __init__(self, label,
                 description=None,
                 data=None,
                 event_type=ReflexEvent.GenericEvent,
                 is_alert = False,
                 timestamp=None):

        # TODO: make sure event_type is an event type and not something else

        if not isinstance(label, six.string_types):
            raise MLOpsException("Invalid 'label' type! Expecting string! label: " + type(label))

        if description is None:
            description = ""

        if not isinstance(description, six.string_types):
            raise MLOpsException("Invalid 'description' type! Expecting string! desc: " + description)

        self.type = event_type
        self.label = label
        self.description = description
        self.data = str(pickle.dumps(data))
        self.is_alert = is_alert
        self.timestamp = timestamp

    """
    This is an internal routine to override the data pickling performed by default
    mlops internal alerts like canary requires specific data format that should not be
    pickled
    """
    def _set_data(self, data):
        self.data = data

    def __str__(self):
        return "Event: type: {} label: {}".format(self.type, self.label)
