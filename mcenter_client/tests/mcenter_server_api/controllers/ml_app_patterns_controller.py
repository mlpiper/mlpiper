import connexion
import six

from mcenter_server_api.models.ml_app_pattern import MLAppPattern  # noqa: E501
from mcenter_server_api import util


def onboarding_ml_app_patterns_get():  # noqa: E501
    """Get list of all MLApp patterns

     # noqa: E501


    :rtype: List[MLAppPattern]
    """
    return 'do some magic!'


def onboarding_ml_app_patterns_ml_app_pattern_id_delete(ml_app_pattern_id):  # noqa: E501
    """Delete an existing MLApp pattern

     # noqa: E501

    :param ml_app_pattern_id: MLApp pattern identifier
    :type ml_app_pattern_id: str

    :rtype: None
    """
    return 'do some magic!'


def onboarding_ml_app_patterns_ml_app_pattern_id_get(ml_app_pattern_id):  # noqa: E501
    """Get specific MLApp pattern

     # noqa: E501

    :param ml_app_pattern_id: MLApp pattern identifier
    :type ml_app_pattern_id: str

    :rtype: MLAppPattern
    """
    return 'do some magic!'


def onboarding_ml_app_patterns_ml_app_pattern_id_put(ml_app_pattern_id, ml_app_pattern):  # noqa: E501
    """Update an existing MLApp pattern

     # noqa: E501

    :param ml_app_pattern_id: MLApp pattern identifier
    :type ml_app_pattern_id: str
    :param ml_app_pattern: MLApp pattern detail configuration
    :type ml_app_pattern: dict | bytes

    :rtype: MLAppPattern
    """
    if connexion.request.is_json:
        ml_app_pattern = MLAppPattern.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def onboarding_ml_app_patterns_post(ml_app_pattern):  # noqa: E501
    """Create a new MLApp pattern

     # noqa: E501

    :param ml_app_pattern: MLApp pattern detail description
    :type ml_app_pattern: dict | bytes

    :rtype: MLAppPattern
    """
    if connexion.request.is_json:
        ml_app_pattern = MLAppPattern.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'
