import connexion
import six
import base64
import os
import time
import flask

from mcenter_server_api.models.group import Group  # noqa: E501
from mcenter_server_api import util

from . import base

groups = dict()


def onboarding_groups_get():  # noqa: E501
    """Get list of all groups

     # noqa: E501


    :rtype: List[Group]
    """
    base.check_session()
    return list(groups.values())


def onboarding_groups_group_id_delete(groupId):  # noqa: E501
    """Delete a group

     # noqa: E501

    :param group_id: Group identifier
    :type group_id: str

    :rtype: None
    """
    base.check_session()
    if groupId not in groups:
        flask.abort(404)
    del groups[groupId]


def onboarding_groups_group_id_get(groupId):  # noqa: E501
    """Get details of specific group

     # noqa: E501

    :param group_id: Group identifier
    :type group_id: str

    :rtype: Group
    """
    base.check_session()
    if groupId not in groups:
        flask.abort(404)
    return groups[groupId]


def onboarding_groups_group_id_put(groupId, body):  # noqa: E501
    """Update group information

     # noqa: E501

    :param group_id: Group identifier
    :type group_id: str
    :param group: Group detail description
    :type group: dict | bytes

    :rtype: Group
    """
    base.check_session()
    if groupId not in groups:
        flask.abort(404)
    group = groups[groupId]
    for k, v in body.items():
        if k == 'name':
            group[k] = v
        elif k in ['id', 'created', 'createdBy']:
            pass  # These are read only
        else:
            group[k] = v
    return group


def onboarding_groups_post(body):  # noqa: E501
    """Add new group

     # noqa: E501

    :param group: Create group
    :type group: dict | bytes

    :rtype: Group
    """
    s = base.check_session()
    a = body.get('name', None)
    if not a:
        flask.abort(500)
    base.finish_creation(body, s, groups)
    return body
