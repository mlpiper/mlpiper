"""
Implementation of REST API client for standalone mode.
"""
try:  # python3
    from urllib.parse import urlencode
except ImportError:  # python2
    from urllib import urlencode

import json
import os
import shutil
import tempfile
import uuid

from parallelm.mlops import Constants
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.mlops_rest_interfaces import MlOpsRestHelper
from parallelm.mlops.models.model import ModelMetadata


class MlOpsRestStandAlone(MlOpsRestHelper):
    """
    This class is a set of helpers to fetch configuration information and models from the MLOps server.
    """

    def __init__(self, mlops_server=Constants.MLOPS_DEFAULT_HOST, mlops_port=Constants.MLOPS_DEFAULT_PORT, token=None):
        super(MlOpsRestHelper, self).__init__(__name__)
        prefix = Constants.OFFICIAL_NAME + "_"
        self._dir = tempfile.mkdtemp(prefix=prefix)
        self._data_dir = os.path.join(self._dir, "model_data")
        self._meta_dir = os.path.join(self._dir, "model_meta")
        os.mkdir(self._data_dir)
        os.mkdir(self._meta_dir)

    def login(self, user='admin', auth='admin'):
        pass

    def get_model_list(self):
        """
        Read all metadata files from the local directory
        :return: list of all file metadata
        """
        all_metadata = []
        for root, dirs, files in os.walk(self._meta_dir):
            for f in files:
                with open(os.path.join(self._meta_dir, f)) as json_data:
                    meta = json.load(json_data)
                    all_metadata.append(meta)
        return all_metadata

    def get_last_approved_model(self, workflow_run_id, pipeline_inst_id):
        os.chdir(self._meta_dir)
        latest_metadata = sorted(filter(os.path.isfile, os.listdir('.')), key=os.path.getmtime)
        return latest_metadata[-1:]

    def post_model_as_file(self, model_file_path, params, metadata):
        """
        Posts a file to the server
        :param model_file_path: model file to upload
        :param params: parameters dictionary
        :param metadata: extended metadata(currently not used with rest connected)
        :return: model_id
        """

        if metadata and not isinstance(metadata, ModelMetadata):
            raise MLOpsException("metadata argument must be a ModelMetadata object, got {}".format(type(metadata)))

        required_params = ["modelId"]

        for param_name in required_params:
            if param_name not in params:
                raise MLOpsException('parameter {} is required for publishing model'.format(param_name))

        model_id = params["modelId"]

        if metadata:
            model_meta_file = os.path.join(self._meta_dir, model_id)
            with open(model_meta_file, 'w') as outfile:
                json.dump(metadata.to_dict(), outfile)

        model_data_file = os.path.join(self._data_dir, model_id)
        with open(model_data_file, "w+") as data_file:
            # maybe should be changed to shutil.copyfile
            data_file.write(open(model_file_path, "r").read())

        return model_id

    def get_ees(self):
        raise NotImplementedError

    def get_agents(self):
        raise NotImplementedError

    def get_workflow_instance(self, ion_instance_id):
        raise NotImplementedError

    def get_health_thresholds(self, ion_instance_id):
        raise NotImplementedError

    def download_model(self, model_id):
        """
        Return the model data with the given ID
        :param model_id: id of the model
        :return: model data
        """
        model_data_file = os.path.join(self._data_dir, model_id)
        with open(model_data_file) as data_file:
            model_data = data_file.read()
            return model_data

    def get_stat(self, stat_name, ion_id, workflow_node_id, agent_id, pipeline_id, start_time, end_time):
        raise NotImplementedError

    def get_model_stats(self, model_id):
        raise NotImplementedError

    def get_alerts(self, query_args):
        raise NotImplementedError

    def done(self):
        shutil.rmtree(self._dir)

    def get_uuid(self, type):
        return "{}_{}".format(type, str(uuid.uuid4()))
