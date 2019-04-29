from enum import Enum


class ComponentModelBehaviorType(Enum):
    MODEL_PRODUCER = "ModelProducer"
    MODEL_CONSUMER = "ModelConsumer"
    MODEL_PRODUCER_CONSUMER = "ModelProducerConsumer"
    AUXILIARY = "Auxiliary"
