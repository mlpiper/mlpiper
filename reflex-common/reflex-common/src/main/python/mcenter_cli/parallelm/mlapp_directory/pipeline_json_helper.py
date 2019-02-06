from parallelm.mlapp_directory.mlapp_defs import PipelineKeywords


class PipelineJSONHelper(object):
    """
    Some helper methods to work with a dictionary created from pipeline json
    """
    def __init__(self, pipeline_json_dict):
        self._pipe = pipeline_json_dict

    @property
    def name(self):
        return self._pipe[PipelineKeywords.NAME]

    @property
    def engine_type(self):
        return self._pipe[PipelineKeywords.ENGINE_TYPE]

    @property
    def pipeline_type(self):
        return self._pipe[PipelineKeywords.PIPELINE_TYPE]