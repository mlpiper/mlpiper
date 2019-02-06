from enum import Enum


class PipelineTypes(Enum):

    MODEL_CONSUMER = "model_consumer"
    MODEL_PRODUCER = "model_producer"
    MODEL_PRODUCER_CONSUMER = "model_producer_consumer"
    COMPARATOR = "pipeline_comparator"
    AUXILIARY = "auxiliary"

    @staticmethod
    def list():
        return [e.value for e in PipelineTypes]

