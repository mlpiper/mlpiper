from enum import Enum


class StatCategory(Enum):
    """
    Types of statistics classes that are supported when calling the MLOps.stat method.
    """

    GENERAL = 0

    #: Configuration value. When set multiple times, the last value will be kept.
    CONFIG = 1

    #: Time series value. When set multiple times, values will be form a series.
    TIME_SERIES = 2

    #: This type represents input data. Computations may be done on this data, such as calculating histograms.
    INPUT = 3


class StatsMode:
    """
    Hint as to how to visualize the data. For example, a series of numbers may be a time series, or a single moment
    in time, such as a bar graph.
    """
    TimeSeries = "TIME_SERIES"
    Instant = "INSTANT"


class StatGraphType:
    """
    Hint as to how to visualize the data.
    """

    HISTOGRAM = "HISTOGRAM"
    HEATMAP = "HEATMAP"
    EVENTS = "EVENTS"
    MATRIX = "MATRIX"
    KPI = "KPI"
    LINEGRAPH = "LINEGRAPH"
    BARGRAPH = "BARGRAPH"
    MULTILINEGRAPH = "MULTILINEGRAPH"
    TABLE = "TABLE"
    OPAQUE = "OPAQUE"
    GENERAL_GRAPH = "GENERAL_GRAPH"
    HTML = "HTML"

    # Note: the below types are not defined in ECO
    UNKNOWN = "UNKNOWN"
    VECTOR = "VECTOR"
    HISTOGRAM_TYPE_CONTIGOUOUS = 'continuous'
    HISTOGRAM_TYPE_CATEGORICAL = 'categorical'

    MLOPS_STAT_GRAPH_TYPES = [BARGRAPH, LINEGRAPH, MULTILINEGRAPH, OPAQUE, MATRIX, GENERAL_GRAPH]


class StatTables:
    """
    Which database table the data should be entered into.
    """
    SYSTEM_STATS = "SYSTEM_STATS"
    CONTINUOUS_HISTOGRAM = "CONTINUOUS_HISTOGRAM"
    CONTINUOUS_HISTOGRAM_COMPARISON = "CONTINUOUS_HISTOGRAM_COMPARISON"
    CATEGORICAL_HISTOGRAM = "CATEGORICAL_HISTOGRAM"
    CATEGORICAL_HISTOGRAM_COMPARISON = "CATEGORICAL_HISTOGRAM_COMPARISON"
    PREDICTION_HISTOGRAM = "PREDICTION_HISTOGRAM"
    DATA_HEATMAP = "DATA_HEATMAP"
    MODEL_STATS = "MODEL_STATS"
    PIPELINE_STATS = "PIPELINE_STATS"
    USER_DEFINED = "USER_DEFINED"
    STAT = "STAT"
    SPARK_STATS_ACCUMULATORS = "SPARK_STATS_ACCUMULATORS"
    REFLEX_PREDICTION = "REFLEX_PREDICTION"
    UNKNOWN = "UNKNOWN"
