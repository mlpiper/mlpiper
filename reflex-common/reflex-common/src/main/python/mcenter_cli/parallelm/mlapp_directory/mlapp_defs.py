

class MLAppDirBuilderDefs:
    MLAPP_MAIN_FILE = "mlapp.json"
    MLAPP_PIPELINES_DIR = "pipelines"
    MLAPP_MODELS_DIR = "models"
    MLAPP_COMPONENTS_DIR = "components"


class MLAppVersions:
    V1 = "1.0"
    V2 = "2.0"


class MLAppPatternKeywords:
    ID = "id"
    NODES = "workflow"
    NODE_ID = "id"
    NAME = "name"
    CRON_SCHEDULE = "cronSchedule"
    PIPELINE_PATTERN_ID = "pipelinePatternId"
    NODE_GROUP_ID = "groupId"

    NODE_CHILDREN = "children"
    NODE_PARENT = "parent"
    NODE_PIPELINE_TYPE = "pipelineType"
    NODE_NO_CHILD_VAL = "-1"
    NODE_NO_PARENT_VAL = "-1"


class MLAppProfileKeywords:
    ID = "id"
    NAME = "name"
    NODES = "workflow"
    IS_PROFILE = "isProfile"
    PATTERN_ID = "ionPatternId"
    PATTERN_NAME = 'ionPatternName'
    MODEL_POLICY = "modelPolicy"
    GLOBAL_TRESHOLD = "globalThreshold"

    NODE_ID = "id"
    NODE_PIPELINE_PATTERN_ID = "pipelinePatternId"
    NODE_PIPELINE_TYPE = "pipelineType"
    NODE_DEFAULT_MODEL_ID = "defaultModelId"
    NODE_GROUP_ID = "groupId"

    NODE_PIPELINE_EE_TUPLE = "pipelineEETuple"
    PIPELINE_EE_TUPLE_PIPELINE_PROFILE_ID = "pipelineProfileId"
    PIPELINE_EE_TUPLE_EE_ENV = "executionEnvironment"

    NODE_PARENT_MCENTER = "-98$7.65#43^21"


class MLAppKeywords:

    VERSION = "version"
    NAME = "name"
    NODES = "nodes"
    PIPELINE = "pipeline"
    CRON_SCHEDULE = "cronSchedule"
    PIPELINE_ID = "pipelineId"
    PIPELINE_PROFILE_ID = "pipelineProfileId"
    GROUP_ID = "groupId"
    GROUP_NAME = "group"

    MODEL_POLICY = "modelPolicy"
    GLOBAL_TRESHOLD = "globalThreshold"

    # Inside the node
    NODE_ID = "id"
    NODE_PARENT = "parent"
    NODE_DEFAULT_MODEL_NAME = "defaultModelName"
    NODE_DEFAULT_MODEL_ID = "defaultModelId"
    NODE_DEFAULT_MODEL_PATH = "defaultModelPath"
    NODE_DEFAULT_MODEL_FORMAT = "defaultModelFormat"

    NODE_AGENT_TO_PROFILE = "agentToProfile"
    NODE_PIPELINE_TYPE = "pipelineType"

    PIPELINE_JSON = "pipeline_json"
    PIPELINE_ENGINE_TYPE = "engineType"
    PIPELINE_NAME = "name"


class PipelineKeywords:
    VERSION = "version"
    NAME = "name"
    ENGINE_TYPE = "engineType"
    PIPELINE_TYPE = "pipelineType"
    PIPE_SECTION = "pipe"
    ARGUMENTS = "arguments"
    COMPONENT_ID = "id"
    COMPONENT_TYPE = "type"
    COMPONENT_TYPE_NAME = "componentName"
    COMPONENT_NAME = "name"
    COMPONENT_PARENTS = "parents"

    PARENT_PARENT = "parent"
    PARENT_OUTPUT = "output"
    PARENT_INPUT = "input"


class PipelineProfileKeywords:
    PIPELINE_SECTION = "pipeline"


class ComponentDescriptionKeywords:
    COMPONENT_DESC_OUTPUT = "outputInfo"
    COMPONENT_DESC_OUTPUT_LABEL = "label"


class GroupKeywords:
    ID = "id"
    NAME = "name"
    AGENTS = "agents"


class ModelsKeywords:
    ID = "id"
    NAME = "name"
