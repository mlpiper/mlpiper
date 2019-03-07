import connexion
import six

from mcenter_server_api.models.pipeline_pattern import PipelinePattern  # noqa: E501
from mcenter_server_api import util


def onboarding_pipeline_patterns_get():  # noqa: E501
    """Get list of all pipeline patterns

     # noqa: E501


    :rtype: List[PipelinePattern]
    """
    return 'do some magic!'


def onboarding_pipeline_patterns_pipeline_pattern_id_delete(pipeline_pattern_id):  # noqa: E501
    """Delete an existing pipeline pattern

     # noqa: E501

    :param pipeline_pattern_id: Pipeline pattern identifier
    :type pipeline_pattern_id: str

    :rtype: None
    """
    return 'do some magic!'


def onboarding_pipeline_patterns_pipeline_pattern_id_get(pipeline_pattern_id):  # noqa: E501
    """Get specific pipeline pattern

     # noqa: E501

    :param pipeline_pattern_id: Pipeline pattern identifier
    :type pipeline_pattern_id: str

    :rtype: PipelinePattern
    """
    return 'do some magic!'


def onboarding_pipeline_patterns_pipeline_pattern_id_put(pipeline_pattern_id, pipeline_pattern):  # noqa: E501
    """Update an existing pipeline pattern

     # noqa: E501

    :param pipeline_pattern_id: Pipeline pattern identifier
    :type pipeline_pattern_id: str
    :param pipeline_pattern: Pipeline pattern detail configuration
    :type pipeline_pattern: dict | bytes

    :rtype: PipelinePattern
    """
    if connexion.request.is_json:
        pipeline_pattern = PipelinePattern.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def onboarding_pipeline_patterns_post(pipeline_pattern):  # noqa: E501
    """Create a new pipeline pattern

     # noqa: E501

    :param pipeline_pattern: Pipeline detail description
    :type pipeline_pattern: dict | bytes

    :rtype: PipelinePattern
    """
    if connexion.request.is_json:
        pipeline_pattern = PipelinePattern.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'
