import connexion
import six

from mcenter_server_api.models.model import Model  # noqa: E501
from mcenter_server_api.models.model_review import ModelReview  # noqa: E501
from mcenter_server_api.models.model_source import ModelSource  # noqa: E501
from mcenter_server_api.models.model_stat import ModelStat  # noqa: E501
from mcenter_server_api.models.model_status import ModelStatus  # noqa: E501
from mcenter_server_api.models.model_usage import ModelUsage  # noqa: E501
from mcenter_server_api import util


def ml_app_instances_ml_app_instance_id_logs_download_get(ml_app_instance_id):  # noqa: E501
    """Download specific model

     # noqa: E501

    :param ml_app_instance_id: MLApp instance identifier
    :type ml_app_instance_id: str

    :rtype: file
    """
    return 'do some magic!'


def models_get():  # noqa: E501
    """Get metadata of models

     # noqa: E501


    :rtype: List[Model]
    """
    return 'do some magic!'


def models_model_id_data_characteristic_get(model_id):  # noqa: E501
    """Fetch model characteristics also know as healths

     # noqa: E501

    :param model_id: Model identifier
    :type model_id: str

    :rtype: List[ModelStat]
    """
    return 'do some magic!'


def models_model_id_delete(model_id):  # noqa: E501
    """Delete a model

     # noqa: E501

    :param model_id: Model identifier
    :type model_id: str

    :rtype: None
    """
    return 'do some magic!'


def models_model_id_download_get(model_id):  # noqa: E501
    """Download specific model

     # noqa: E501

    :param model_id: Model identifier
    :type model_id: str

    :rtype: file
    """
    return 'do some magic!'


def models_model_id_get(model_id):  # noqa: E501
    """Return metadata of specific model

     # noqa: E501

    :param model_id: Model identifier
    :type model_id: str

    :rtype: Model
    """
    return 'do some magic!'


def models_model_id_review_post(model_id, model_review):  # noqa: E501
    """Review a model approve/reject for current mlApp

     # noqa: E501

    :param model_id: Model identifier
    :type model_id: str
    :param model_review: Model review parameters
    :type model_review: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        model_review = ModelReview.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def models_model_id_source_get(model_id):  # noqa: E501
    """Fetch model provenance

     # noqa: E501

    :param model_id: Model identifier
    :type model_id: str

    :rtype: List[ModelSource]
    """
    return 'do some magic!'


def models_model_id_status_get(model_id):  # noqa: E501
    """Fetch model status, list of mlApp approved/rejected for

     # noqa: E501

    :param model_id: Model identifier
    :type model_id: str

    :rtype: List[ModelStatus]
    """
    return 'do some magic!'


def models_model_id_usage_get(model_id):  # noqa: E501
    """Fetch model usages time linked to pipelines

     # noqa: E501

    :param model_id: Model identifier
    :type model_id: str

    :rtype: List[ModelUsage]
    """
    return 'do some magic!'


def models_post():  # noqa: E501
    """Upload a model

     # noqa: E501


    :rtype: Model
    """
    return 'do some magic!'
