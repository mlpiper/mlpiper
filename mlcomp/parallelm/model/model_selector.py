import os

from parallelm.model.metadata import Metadata
from parallelm.model.model_env import ModelEnv


class ModelSelector(object):
    def __init__(self, model_filepath, standalone=False):
        self._model_env = ModelEnv(model_filepath, standalone)

    @property
    def model_env(self):
        return self._model_env

    def pick_model_filepath(self):
        model_filepath = None
        if self._model_env.standalone:
            model_filepath = self._model_env.model_filepath
        else:
            if os.path.isfile(self._model_env.metadata_filepath):
                model_filepath = Metadata().load(self._model_env.metadata_filepath).model_filepath

        return model_filepath

    # TODO: change to pick_model_metadata using ModelMetadata
    def pick_model_id(self):
        model_id = None
        if self._model_env.standalone:
            model_id = None
        else:
            if os.path.isfile(self._model_env.metadata_filepath):
                model_id = Metadata().load(self._model_env.metadata_filepath).get_id()

        return model_id
