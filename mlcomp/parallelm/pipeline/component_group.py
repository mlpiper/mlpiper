from enum import Enum


class ComponentGroup(Enum):
    CONNECTORS = "Connectors"
    SINKS = "Sinks"
    FEATURE_ENG = "FeatureEng"
    ALGORITHMS = "Algorithms"
    GENERAL = "General"
    FLOW_SHAPING = "Flow Shaping"
    DATA_PARSERS = "DataParser"
    DATA_GENERATORS = "DataGenerators"
    STATISTICS = "Statistics"
    MODEL_CONNECTORS = "ModelConnectors"
