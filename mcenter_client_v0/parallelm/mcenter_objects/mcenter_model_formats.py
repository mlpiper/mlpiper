from enum import Enum


class MCenterModelFormats(Enum):

    PMML = "Pmml"
    JSON = "Json"
    SPARKML = "SparkML"
    SAVEDMODEL = "Savedmodel"  # TensorFlow model
    BINARY = "Binary"
    TEXT = "Text"

    @staticmethod
    def list():
        return [e.value for e in MCenterModelFormats]
