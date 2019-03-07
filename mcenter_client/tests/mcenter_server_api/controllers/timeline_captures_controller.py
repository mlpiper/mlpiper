import connexion
import six

from mcenter_server_api.models.inline_response2001 import InlineResponse2001  # noqa: E501
from mcenter_server_api.models.timeline_capture import TimelineCapture  # noqa: E501
from mcenter_server_api import util


def timeline_captures_get():  # noqa: E501
    """Get list of all timeline captures

     # noqa: E501


    :rtype: List[TimelineCapture]
    """
    return 'do some magic!'


def timeline_captures_post(timeline_capture):  # noqa: E501
    """Create a timeline capture

     # noqa: E501

    :param timeline_capture: Timeline capture details
    :type timeline_capture: dict | bytes

    :rtype: TimelineCapture
    """
    if connexion.request.is_json:
        timeline_capture = TimelineCapture.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def timeline_captures_timeline_capture_id_delete(timeline_capture_id):  # noqa: E501
    """Delete a timeline capture

     # noqa: E501

    :param timeline_capture_id: Timeline capture identifier
    :type timeline_capture_id: str

    :rtype: None
    """
    return 'do some magic!'


def timeline_captures_timeline_capture_id_download_get(timeline_capture_id):  # noqa: E501
    """Download specific model

     # noqa: E501

    :param timeline_capture_id: Timeline capture identifier
    :type timeline_capture_id: str

    :rtype: file
    """
    return 'do some magic!'


def timeline_captures_timeline_capture_id_get(timeline_capture_id):  # noqa: E501
    """Return details of specific timeline capture

     # noqa: E501

    :param timeline_capture_id: Timeline capture identifier
    :type timeline_capture_id: str

    :rtype: TimelineCapture
    """
    return 'do some magic!'


def timeline_captures_timeline_capture_id_prepare_post(timeline_capture_id):  # noqa: E501
    """Prepare timeline capture for download

     # noqa: E501

    :param timeline_capture_id: Timeline capture identifier
    :type timeline_capture_id: str

    :rtype: None
    """
    return 'do some magic!'


def timeline_captures_timeline_capture_id_prepare_status_get(timeline_capture_id):  # noqa: E501
    """Return status of timeline capture for download

     # noqa: E501

    :param timeline_capture_id: Timeline capture identifier
    :type timeline_capture_id: str

    :rtype: InlineResponse2001
    """
    return 'do some magic!'
