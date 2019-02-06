import os

from parallelm.model.metadata import Metadata
from parallelm.model.model_env import ModelEnv


class ModelSelector(object):
    def __init__(self, model_filepath):
        self._model_env = ModelEnv(model_filepath)

    @property
    def model_env(self):
        return self._model_env

    def pick_model_filepath(self):
        if not os.path.isfile(self._model_env.metadata_filepath):
            return None

        return Metadata().load(self._model_env.metadata_filepath).model_filepath

