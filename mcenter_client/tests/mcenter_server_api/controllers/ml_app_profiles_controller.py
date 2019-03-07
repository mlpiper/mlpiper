import connexion
import six

from mcenter_server_api.models.ml_app_profile import MLAppProfile  # noqa: E501
from mcenter_server_api import util


def onboarding_ml_app_profiles_get():  # noqa: E501
    """Get list of all MLApp profiles

     # noqa: E501


    :rtype: List[MLAppProfile]
    """
    return 'do some magic!'


def onboarding_ml_app_profiles_ml_app_profile_id_delete(ml_app_profile_id):  # noqa: E501
    """Delete an existing MLApp profile

     # noqa: E501

    :param ml_app_profile_id: MLApp profile identifier
    :type ml_app_profile_id: str

    :rtype: None
    """
    return 'do some magic!'


def onboarding_ml_app_profiles_ml_app_profile_id_get(ml_app_profile_id):  # noqa: E501
    """Get specific MLApp profile

     # noqa: E501

    :param ml_app_profile_id: MLApp profile identifier
    :type ml_app_profile_id: str

    :rtype: MLAppProfile
    """
    return 'do some magic!'


def onboarding_ml_app_profiles_ml_app_profile_id_put(ml_app_profile_id, ml_app_profile):  # noqa: E501
    """Update an existing MLApp profile

     # noqa: E501

    :param ml_app_profile_id: MLApp profile identifier
    :type ml_app_profile_id: str
    :param ml_app_profile: MLApp profile detail configuration
    :type ml_app_profile: dict | bytes

    :rtype: MLAppProfile
    """
    if connexion.request.is_json:
        ml_app_profile = MLAppProfile.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def onboarding_ml_app_profiles_post(ml_app_profile):  # noqa: E501
    """Create a new MLApp profile

     # noqa: E501

    :param ml_app_profile: MLApp profile detail description
    :type ml_app_profile: dict | bytes

    :rtype: MLAppProfile
    """
    if connexion.request.is_json:
        ml_app_profile = MLAppProfile.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'
