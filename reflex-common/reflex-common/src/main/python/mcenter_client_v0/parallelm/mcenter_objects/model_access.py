import logging
import os

from parallelm.mcenter_objects.mcenter_exceptions import InitializationException
from parallelm.mcenter_objects.mcenter_model_formats import MCenterModelFormats


class ModelAccess:
    """
    The class is responsible to read models from MCenter, as well as to upload new ones
    """
    def __init__(self, mclient, mlapp_dir, models=None):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._mclient = mclient
        self._mlapp_dir = mlapp_dir
        self._model_id_map = {}
        self._setup(models)

    def _setup(self, models):
        self._fetch_models()
        if models:
            for model in models:
                try:
                    model_name = model['name']
                    model_path = model['path']
                except KeyError as e:
                    msg = "Missing mandatory key models section definition! {}".format(str(e))
                    self._logger.error(msg)
                    raise InitializationException(msg)

                if model_name in self._model_id_map:
                    self._logger.info("Skip model uploading, because its name already exists! "
                                      "name: '{}'".format(model_name))
                    continue

                if not os.path.isabs(model_path):
                    model_path = os.path.abspath(os.path.join(self._mlapp_dir, model_path))

                if not os.path.isfile(model_path):
                    msg = "Model path not found! path: {}".format(model_path)
                    self._logger.error(msg)
                    raise InitializationException(msg)

                model_id = self._mclient.upload_model(model_name, model_path, MCenterModelFormats.DEFAULT_MODEL_FORMAT)
                if not model_id:
                    msg = "Failed to upload model! model name: {}, model path: {}".format(model_name, model_path)
                    self._logger.error(msg)
                    raise InitializationException(msg)

                self._model_id_map[model_name] = model_id
                self._logger.info("Uploaded model! name: {}, id: {}".format(model_name, model_id))
        return self

    def model_id(self, name):
        try:
            return self._model_id_map[name]
        except KeyError as e:
            self._fetch_models()
            try:
                return self._model_id_map[name]
            except KeyError as e:
                msg = "Tried to refer to non-existing model! name: {}".format(name)
                self._logger.error(msg)
                raise InitializationException(msg)

    def _fetch_models(self):
        models = self._mclient.get_models()
        if models:
            for model in models:
                self._model_id_map[model['name']] = model['id']

