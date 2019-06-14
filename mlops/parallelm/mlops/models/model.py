import os
import io
import six
import json
from enum import Enum

from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.models.mlobject import MLObject
from parallelm.mlops.models.mlobject import MLObjectType
from parallelm.mlops.stats.stats_helper import StatsHelper
from parallelm.mlops.stats_category import StatCategory
from parallelm.mlops import models


class ModelFormat(str, Enum):
    JSON = "Json"
    SAVEDMODEL = "Savedmodel"
    SPARKML = "SparkML"
    BINARY = "Binary"
    TEXT = "Text"
    SCIKIT_LEARN_2 = "ScikitLearn_2"
    SCIKIT_LEARN_3 = "ScikitLearn_3"
    H2O_3 = "H2O_3"
    H2O_DRIVERLESS_AI = "H2O_Driverless_AI"
    UNKNOWN = "Unknown"

    @classmethod
    def from_str(cls, name):
        for e in cls:
            if e.name.lower() == name.lower():
                return e


class ModelMetadata(object):
    """
    Model related metadata
    """

    def __init__(self, modelId, name, model_format, description=""):
        if model_format and not isinstance(model_format, ModelFormat):
            raise MLOpsException("model_format object must be an instance of ModelFormat class! provided: "
                                 "{}, type: {}".format(model_format, type(model_format)))
        if model_format == ModelFormat.UNKNOWN:
            raise MLOpsException(
                "model_format can not be {}. Did you forget to set a format for model?".format(model_format.value))
        self.modelId = modelId
        self.name = name
        self.modelFormat = model_format
        self.description = description
        self.annotations = {}

        # these fields are set by MCenter server
        self.size = None
        self.owner = ""
        self.train_version = ""
        self.model_version = ""
        self.active = False
        self.created_on = None
        self.flag_values = []

    def __str__(self):
        return json.dumps(self.to_dict())

    def __eq__(self, other):
        """
        Implements a naive equal comparison. Yet to be improved.

        :param other: a model metadata
        :return: True if model id are equal else False
        """
        if isinstance(other, ModelMetadata):
            return self.modelId == other.modelId

        return NotImplemented

    def __ne__(self, other):
        result = self.__eq__(other)
        if result is NotImplemented:
            return result

        return not result

    def to_dict(self):
        return {
                models.json_fields.MODEL_ID_FIELD: self.modelId,
                models.json_fields.MODEL_NAME_FIELD: self.name,
                models.json_fields.MODEL_FORMAT_FIELD: self.modelFormat.value,
                models.json_fields.MODEL_VERSION_FIELD: self.model_version,
                models.json_fields.MODEL_TRAIN_VERSION_FIELD: self.train_version,
                models.json_fields.MODEL_SIZE_FIELD: self.size,
                models.json_fields.MODEL_OWNER_FIELD: self.owner,
                models.json_fields.MODEL_CREATED_ON_FIELD: self.created_on,
                models.json_fields.MODEL_FLAG_VALUES_FIELD: self.flag_values,
                models.json_fields.MODEL_ANNOTATIONS_FIELD: self.annotations,
                models.json_fields.MODEL_ACTIVE_FIELD: self.active,
                models.json_fields.MODEL_DESCRIPTION_FIELD: self.description
            }


class Model(MLObject):
    """
    This class provides APIs to access Model related data, publish model, attach statistics to model.
    """

    def __init__(self, stats_helper, rest_helper, name, model_format, description, id=None):
        super(Model, self).__init__(rest_helper, id)

        model_id = self.get_id()
        if model_id is None or not isinstance(model_id, six.string_types) or model_id == "":
            raise MLOpsException('model id must be non zero valid string type, received: {}'.format(model_id))

        self.model_path = None
        self.metadata = ModelMetadata(self.get_id(), name, model_format, description)
        if stats_helper and not isinstance(stats_helper, StatsHelper):
            raise MLOpsException("stats_helper object must be an instance of StatsHelper class")
        self._stats_helper = stats_helper

        self._pipeline_instance_id = None
        self._path_to_publish = None

    def __eq__(self, other):
        """
        Models are regarded as equal if their metadata are equal

        :param other: another model
        :return: True if model's metadata are equal else False
        """
        return self.metadata == other.metadata

    def __ne__(self, other):
        """
        Models are regarded as not equal if their metadata are not equal

        :param other: another model
        :return: True if model's metadata are not equal else False
        """
        return self.metadata != other.metadata

    def _get_object_type(self):
        return MLObjectType.MODEL

    def __str__(self):
        return self.metadata.__str__()

    def _validate_stats_helper(self):
        if not self._stats_helper:
            raise MLOpsException("stats_helper object was not set or is None")

    def set_model_path(self, path):
        self.model_path = path
        self.metadata.size = os.stat(path).st_size

    def get_model_path(self):
        return self.model_path

    def set_annotations(self, annotations):
        if annotations is None or not isinstance(annotations, dict):
            raise MLOpsException("Model annotations must be not None dict")
        self.metadata.annotations = annotations

    def get_annotations(self):
        return self.metadata.annotations

    def set_stat(self, name, data=None, category=StatCategory.TIME_SERIES, timestamp=None, **kwargs):
        """
        Report this statistic.
        Statistic is attached to the current model and can be fetched later for this model.

        :param name: name to use in the export
        :param data: data object to export
        :param category: category of the statistic. One of :class:`StatsCategory`
        :param timestamp: optional timestamp
        :raises: MLOpsException
        """
        self._validate_stats_helper()
        self._stats_helper.set_stat(name, data, self.get_id(), category, timestamp)

    def set_data_distribution_stat(self, data, model=None, timestamp=None):
        """
        Exports distribution statistics which will be shown in Health View.
        Statistic is attached to the current model and can be fetched later for this model.

        :param data: The data that represents distribution. Data must have specific type according to engine.
                     For PyStark engine: RDD or DataFrame.
                     For Python engine: Numpy ND array or Pandas DataFrame
                     Currently the only expected data type is a line graph, which consists of
                     discrete numeric values
        :param model: For PySpark engine: model is used to classify categorical and continuous features.
        :param timestamp: The timestamp is a given units (Optional). If not provided, the current time is assumed
        :raises: MLOpsException
        """
        self._validate_stats_helper()
        self._stats_helper.set_data_distribution_stat(data, self.get_id(), model, timestamp)

    def set_kpi(self, name, data, timestamp=None, units=None):
        """
        Exports KPI data to the PM service. Users may supply a timestamp, which allows older data to be loaded.
        Statistic is attached to the current model and can be fetched later for this model.

        :param name: KPI name, which will be displayed in the UI. It can be used to fetch the stored data
                     at a later time.
        :param data: The data to store. Currently the only expected data type is a line graph, which consists of
                     discrete numeric values
        :param timestamp: The timestamp is a given units (Optional). If not provided, the current time is assumed
        :param units: The timestamp units. One of: KpiValue.TIME_SEC, KpiValue.TIME_MSEC, KpiValue.TIME_NSEC
        :return: The current PM instance for further calls
        :raises: MLOpsException
        """
        self._validate_stats_helper()
        self._stats_helper.set_kpi(name, data, self.get_id(), timestamp, units)

    def feature_importance(self, feature_importance_vector=None, feature_names=None, model=None, df=None,
                           num_significant_features=8):
        """
        present feature importance, either according to the provided vector or generated from
        the provided model if available.
        Feature importance bar graph is attached to the current model and can be fetched later for
         this model.
        this function implements:
        1) use feature_importance_vector if exists
        2) feature_names from the model if available

        3) get feature names vector if exists
        4) extract feature name from pipeline model or dataframe if exists -
         (code different to pyspark and sklearn)

        5) sort the vector.
        6) take first k elements
        7) create a bar graph for feature importance

        :param feature_importance_vector: feature importance vector optional
        :param feature_names: feature names vector optional
        :param model: optional pipeline model for pyspark, sklearn model for python
        :param df: optional dataframe for analysis
        :raises: MLOpsException
        """
        self._validate_stats_helper()
        self._stats_helper.feature_importance(self, feature_importance_vector, feature_names, model, df,
                                              num_significant_features)

    def download(self, filepath):
        """
        Download the model content specified by this model metadata and save it on the local file system.
        Note model size might be big, check the expected model size before downloading it.

        :param: filepath  the file path in the local file system to save the model's content
        """
        content = self._rest_helper.download_model(self.get_id())

        # In case the model was created from a json response of get model REST API
        if self.metadata.size and self.metadata.size != len(content):
            raise MLOpsException("Unexpected downloaded model size! model id: {}, expected size: {},"
                                 " downloaded size: {}".format(self.get_id(), self.metadata.size, len(content)))

        with io.open(filepath, mode='wb') as f:
            f.write(content)

        self.set_model_path(filepath)
