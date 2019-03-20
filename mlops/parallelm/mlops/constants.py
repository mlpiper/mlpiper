from parallelm.mlops.versions import Versions


class Constants:
    """
    Various constants related to MLOps
    """

    OFFICIAL_NAME = "ml-ops"

    MLOPS_CURRENT_VERSION = Versions.VERSION_1_0_1

    # Port for receiving stats from MLOps
    REST_SERVER_DEFAULT_PORT = 8889

    # Configuration for sending statistics/events to MLOps
    MLOPS_DEFAULT_PORT = "3456"
    MLOPS_DEFAULT_HOST = "localhost"
    MLOPS_DEFAULT_USER = "admin"

    SERVER_PROTOCOL = "http"
    URL_SCHEME = ":"
    URL_HIER_PART = "//"
    URL_MLOPS_PREFIX = "mlops"

    # MLOps Zookeeper
    MLOPS_ZK_ACTIVE_HOST_PORT = '/ECO/curator/activeHostPort'

    # MLOps constants used to parse data coming from mlops_rest_interfaces.MlOpsRestHelper.get_stat
    STAT_MLSTAT = "mlStat"
    STAT_VALUES = "values"
    STAT_GRAPHTYPE = "graphType"
    STAT_COLUMNS = "columns"
    STAT_NAME = "name"
    MIN_STAT_COLUMNS = 2

    # The number of seconds to wait for the pipelineInstances part of the ion info to appear in the json/dict
    WAIT_FOR_PIPELINE_INSTANCE_TO_APPEAR_TIMEOUT = 60

    # General multi-graph types
    STAT_CONTINUOUS = "Continuous"
    STAT_CATEGORICAL = "Categorical"

    # ION User visible literal
    ION_LITERAL = "ML App"


class HTTPStatus:
    OK = 200
    SERVICE_UNAVAIL = 503


class MLOpsRestHandles:
    LOGIN = "auth/login"
    GROUPS = "groups"
    AGENTS = "agents"
    EES = 'executionEnvironments'
    MODELS = "models"
    WORKFLOW_INSTANCES_V2 = "workflows/instances"
    HEALTH_THRESHOLDS = "health/thresholds"
    DOWNLOAD = "download"
    STATS = "stats"
    MODEL_STATS = "modelStats"
    EVENTS = "events"
    UUID = "uuid"


class DataframeColNames:
    ION_NODE = "node"
    AGENT = "agent"
    OPAQUE_DATA = "data"


class HistogramType:
    """
    Constants used for histogram parsing
    """
    HISTOGRAM_COLUMN = "hist_values"
    BIN_EDGES_COLUMN = "bin_edges"
    HISTOGRAM_TYPE_COLUMN = "hist_type"
    STAT_NAME_COLUMN = "stat_name"
    TIMESTAMP_COLUMN = "time"
    HISTOGRAM_BIN_IDENTIFIER = " to "

class MatrixType:
    """
    Constants used for matrix parsing
    """
    MATRIX_COLUMN = "matrix_columns"
    MATRIX_VALUES = "matrix_values"
    MATRIX_ROW_NAME = "matrix_row_name"

class GeneralType:
    """
    Constants used for matrix parsing
    """
    YSERIES = "y_series"
    DATA = "data"
    LABEL = "label"

class PyHealth:
    MAXIMUM_CATEGORY_UNIQUE_VALUE_REQ = 25
    HISTOGRAM_OVERLAP_SCORE_PREPENDER = "overlapScore_"

    CONTINUOUS_HISTOGRAM_OVERLAP_SCORE_KEY = "continuousDataHistogram.overlapResult"
    CATEGORICAL_HISTOGRAM_OVERLAP_SCORE_KEY = "categoricalDataHistogram.overlapResult"

    HEATMAP_KEY = "dataheatmap"

    CONTINUOUS_HISTOGRAM_KEY = "continuousDataHistogram"
    CATEGORICAL_HISTOGRAM_KEY = "categoricalDataHistogram"
