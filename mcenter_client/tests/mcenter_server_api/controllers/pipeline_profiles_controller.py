import connexion
import six

from mcenter_server_api.models.pipeline_profile import PipelineProfile  # noqa: E501
from mcenter_server_api import util


def onboarding_pipeline_profiles_get():  # noqa: E501
    """Get list of all pipeline profiles

     # noqa: E501


    :rtype: List[PipelineProfile]
    """
    return 'do some magic!'


def onboarding_pipeline_profiles_pipeline_profile_id_delete(pipeline_profile_id):  # noqa: E501
    """Delete an existing pipeline profile

     # noqa: E501

    :param pipeline_profile_id: Pipeline profile identifier
    :type pipeline_profile_id: str

    :rtype: None
    """
    return 'do some magic!'


def onboarding_pipeline_profiles_pipeline_profile_id_get(pipeline_profile_id):  # noqa: E501
    """Get list of pipeline profiles

     # noqa: E501

    :param pipeline_profile_id: Pipeline profile identifier
    :type pipeline_profile_id: str

    :rtype: List[PipelineProfile]
    """
    return 'do some magic!'


def onboarding_pipeline_profiles_pipeline_profile_id_put(pipeline_profile_id, pipeline_profile):  # noqa: E501
    """Update an existing pipeline profile

     # noqa: E501

    :param pipeline_profile_id: Pipeline profile identifier
    :type pipeline_profile_id: str
    :param pipeline_profile: Pipeline profile detail configuration
    :type pipeline_profile: dict | bytes

    :rtype: PipelineProfile
    """
    if connexion.request.is_json:
        pipeline_profile = PipelineProfile.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def onboarding_pipeline_profiles_post(pipeline_profile):  # noqa: E501
    """Create a new pipeline profile

     # noqa: E501

    :param pipeline_profile: Pipeline detail description
    :type pipeline_profile: dict | bytes

    :rtype: PipelineProfile
    """
    if connexion.request.is_json:
        pipeline_profile = PipelineProfile.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'
