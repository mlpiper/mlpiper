import connexion
import six

from mcenter_server_api.models.event import Event  # noqa: E501
from mcenter_server_api.models.inline_response2002 import InlineResponse2002  # noqa: E501
from mcenter_server_api import util


def alerts_get():  # noqa: E501
    """Get list of alerts

     # noqa: E501


    :rtype: InlineResponse2002
    """
    return 'do some magic!'


def events_get():  # noqa: E501
    """Get list of events

     # noqa: E501


    :rtype: List[Event]
    """
    return 'do some magic!'
