from parallelm.protobuf.ReflexEvent_pb2 import ReflexEvent


class EventType:

    #: Health alert. Describes metrics on ML health
    Health = ReflexEvent.GenericHealthAlert

    #: Data alert. Describes anomalous input
    Data = ReflexEvent.GenericDataAlert

    #: System alert. Describes system state, such as out of memory
    System = ReflexEvent.GenericSystemAlert

    #: Canary health. Describes the difference between the predictions of one pipeline against a baseline
    CanaryHealth = ReflexEvent.CanaryHealth
