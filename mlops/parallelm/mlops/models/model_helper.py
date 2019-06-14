import json
import os
import pandas as pd
import six

from parallelm.mlops.base_obj import BaseObj
from parallelm.mlops.constants import Constants
from parallelm.mlops.models.model import Model
from parallelm.mlops.models.model import ModelFormat
from parallelm.mlops.models import json_fields
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.packer import DirectoryPack
from parallelm.mlops.utils import datetime_to_timestamp_milli


class ModelHelper(BaseObj):
    def __init__(self, rest_helper, ion, stats_helper):
        super(ModelHelper, self).__init__(__name__)
        self._rest_helper = rest_helper
        self._all_models_dict = None
        self._ion = ion
        self._stats_helper = stats_helper

    def fetch_all_models_json_dict(self):
        model_list = self._rest_helper.get_model_list()
        self._debug(str(model_list))
        return model_list

    def convert_models_json_dict_to_dataframe(self, json_dict=None):
        self._debug("Converting json dict to dataframe:\n" + str(json_dict))
        return pd.read_json(json.dumps(json_dict))

    def dataframe_to_object_list(self, models_dataframe):
        object_list = []
        for index, row in models_dataframe.iterrows():
            m = self.create_model(name=row[json_fields.MODEL_NAME_FIELD], model_format=ModelFormat.UNKNOWN)
            m.matadata.format = row[json_fields.MODEL_FORMAT_FIELD]
            m.creation_time = row[json_fields.MODEL_CREATED_ON_FIELD]
            m.id = row[json_fields.MODEL_ID_FIELD]
            if 'data' in models_dataframe.columns:
                m.data = row["data"]
            else:
                m.data = None

            object_list.append(m)
        return object_list

    def object_list_to_dataframe(self):
        pass

    def get_models_dataframe(self, model_filter=None, download=False):
        """
        Get list of models which were created between start-end

        :param model_filter: ModelFilter object to use for filtering models
        :type model_filter: ModelFilter
        :param download: If true download models content
        :type download: bool
        :return: A dataframe containing each model as a row

        Note: the default value of the download parameter is false. You can use the download_model method
        to download specific model. Models size might take considerable amounts of memory.
        """
        self._info("{} id: {}".format(Constants.ION_LITERAL, self._ion.id))
        self._all_models_dict = self.fetch_all_models_json_dict()
        self._info("All models: {}".format(self._all_models_dict))
        df = self.convert_models_json_dict_to_dataframe(self._all_models_dict)

        if len(df) == 0:
            self._info("Models dataframe is empty")
            return df

        self._debug("DF\n{}".format(df))

        if model_filter is not None:
            self._info("Got filter: {}".format(model_filter))

            query = ""
            num_cols = 0

            if model_filter.time_window_start is not None:
                start_ts = datetime_to_timestamp_milli(model_filter.time_window_start)
                if num_cols > 0:
                    query += " &"
                query += " {} >= @start_ts".format(json_fields.MODEL_CREATED_ON_FIELD)
                num_cols += 1

            if model_filter.time_window_end is not None:
                end_ts = datetime_to_timestamp_milli(model_filter.time_window_end)
                if num_cols > 0:
                    query += " &"
                query += " {} <= @end_ts".format(json_fields.MODEL_CREATED_ON_FIELD)
                num_cols += 1

            if model_filter.id is not None:
                if num_cols > 0:
                    query += " &"
                query += " {} == @model_filter.id".format(json_fields.MODEL_ID_FIELD)
                num_cols += 1

            self._info("\nQUERY: {}".format(query))
            df_filtered = df.query(query)

        else:
            self._debug("No filter given - filtering by {} id only".format(Constants.ION_LITERAL))
            df_filtered = df[(df['workflowRunId'] == self._ion.id)]

        if download is True:
            self._info("Downloading models - this might take some time")
            models_content = []
            model_id_list = df_filtered[json_fields.MODEL_ID_FIELD].values
            for model_id in model_id_list:
                self._info("Downloading model: {}".format(model_id))
                models_content.append(self._rest_helper.download_model(model_id))

            df_filtered = df_filtered.assign(data=models_content)

        return df_filtered

    def get_last_approved_model(self, workflow_run_id, pipeline_inst_id):
        self._logger.debug("Getting latest model, workflow_run_id: {}, pipeline_inst_id: {}"
                           .format(workflow_run_id, pipeline_inst_id))
        model_data = self._rest_helper.get_last_approved_model(workflow_run_id, pipeline_inst_id)
        if not model_data:
            return None

        model_dict = model_data[0]
        if not model_dict:
            return None
        model = self.create_model_from_json(model_dict)
        self._logger.debug("Model: {}".format(model))
        return model

    def get_models_object_list(self, model_filter=None, download=False):
        df = self.get_models_dataframe(model_filter, download)
        return self.dataframe_to_object_list(df)

    def download_model(self, model_id):
        """
        Download a specific model.
        Note model size might be big, check the expected model size before downloading it.

        :param model_id:
        :return: Model Object with model member field pointing to model data
        """
        if isinstance(model_id, six.string_types):
            model_data = self._rest_helper.download_model(model_id)
            return model_data
        elif isinstance(model_id, Model):
            model_id.data = self._rest_helper.download_model(model_id.id)
            return model_id
        else:
            raise MLOpsException("model_id argument should be either model_id string, or Model object, got {}".format(
                type(model_id)
            ))

    def get_model_obj(self, model_id):
        all_models_dict = self.fetch_all_models_json_dict()

        for model_dict in all_models_dict:
            if model_dict['id'] == model_id:
                return self.create_model_from_json(model_dict)

        return None

    def get_model_stat(self, model_id):
        hist_json = self._rest_helper.get_model_stats(model_id=model_id)
        json_list = []
        for model_stat in hist_json:
            acc_data = model_stat['data']
            json_list.append(acc_data)
        return json_list

    def create_model(self, name, model_format, description="", id=None):
        return Model(self._stats_helper, self._rest_helper, name, model_format, description, id=id)

    def create_model_from_json(self, model_dict):
        model_format = ModelFormat.from_str(model_dict[json_fields.MODEL_FORMAT_FIELD])
        model = self.create_model(name=model_dict[json_fields.MODEL_NAME_FIELD],
                                  model_format=model_format,
                                  description=model_dict.get(json_fields.MODEL_DESCRIPTION_FIELD),
                                  id=model_dict[json_fields.MODEL_ID_FIELD])
        model.metadata.created_on = model_dict[json_fields.MODEL_CREATED_ON_FIELD]
        model.metadata.size = model_dict[json_fields.MODEL_SIZE_FIELD]

        model.metadata.owner = model_dict[json_fields.MODEL_OWNER_FIELD]
        model.metadata.train_version = model_dict[json_fields.MODEL_TRAIN_VERSION_FIELD]
        model.metadata.model_version = model_dict[json_fields.MODEL_VERSION_FIELD]
        model.metadata.active = model_dict[json_fields.MODEL_ACTIVE_FIELD]
        model.metadata.flag_values = model_dict[json_fields.MODEL_FLAG_VALUES_FIELD]

        model.set_annotations(model_dict[json_fields.MODEL_ANNOTATIONS_FIELD])
        return model

    def publish_model(self, model, pipelineInstanceId):
        if not isinstance(model, Model):
            raise MLOpsException("model argument must be a Model object got {}".format(type(model)))

        model_file_path = model.get_model_path()
        if not os.path.exists(model_file_path):
            raise MLOpsException("publish_model() api works only for models saved to file. Use model.set_model_path() API")

        model_file_to_publish = None

        if model.metadata.modelFormat in [ModelFormat.SAVEDMODEL, ModelFormat.SPARKML]:
            if os.path.isdir(model_file_path):
                dp = DirectoryPack()
                model_file_to_publish = dp.pack(model_file_path)
            else:
                raise MLOpsException("Path to model with format {} expected to be a directory".format(model.metadata.modelFormat.value))
        else:
            if os.path.isfile(model_file_path):
                model_file_to_publish = model_file_path
            else:
                raise MLOpsException("Path to model with format {} expected to be a file".format(model.metadata.modelFormat.value))

        model._pipeline_instance_id = pipelineInstanceId
        model._path_to_publish = model_file_to_publish

        ret = self._rest_helper.post_model_as_file(model)
        if os.path.isdir(model_file_path):
            os.remove(model_file_to_publish)
        return ret
