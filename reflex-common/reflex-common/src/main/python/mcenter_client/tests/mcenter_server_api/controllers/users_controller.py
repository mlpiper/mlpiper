import connexion
import six
import flask
import copy
import os
import base64
import time

from mcenter_server_api.models.inline_response200 import InlineResponse200  # noqa: E501
from mcenter_server_api.models.inline_response2001 import InlineResponse2001  # noqa: E501
from mcenter_server_api.models.user import User  # noqa: E501
from mcenter_server_api import util

from . import base

users = dict(AdminID=dict(username='admin', password='admin',
                          createdBy='admin', created=0, id='AdminID'))


def _cleanuser(u):
    u = copy.copy(u)
    del u['password']
    return u


def _finduserbyname(username):
    for u in users.values():
        if u['username'] == username:
            return u
    return None


def _finduser(userId):
    if userId in users:
        return users[userId]
    flask.abort(404)


def auth_login_post(body):  # noqa: E501
    """Authenticate user

     # noqa: E501

    :param user: username and password fields for authentication
    :type user: dict | bytes

    :rtype: InlineResponse200
    """
    u = _finduserbyname(body['username'])
    if u is None:
        flask.abort(403)
    if u['password'] != body['password']:
        flask.abort(403)
    return dict(token=base.add_session(u))


def auth_validate_post(body):  # noqa: E501
    """Register an user

     # noqa: E501

    :param authorization: Bearer Token
    :type authorization: str

    :rtype: InlineResponse2001
    """
    return 'do some magic!'


def me_get():  # noqa: E501
    """Get user detail of current user

     # noqa: E501


    :rtype: User
    """
    s = base.check_session()
    return _cleanuser(s['user'])


def users_get():  # noqa: E501
    """Get list of users

     # noqa: E501


    :rtype: List[User]
    """
    base.check_session()
    ret = []
    for u in users.values():
        ret.append(_cleanuser(u))
    return ret


def users_post(body):  # noqa: E501
    """Create a new user

     # noqa: E501

    :param user: User detail description
    :type user: dict | bytes

    :rtype: User
    """
    s = base.check_session()
    if _finduserbyname(body['username']) is not None:
        flask.abort(500)
    if not body['password']:
        flask.abort(500)
    base.finish_creation(body, s, users)
    return _cleanuser(body)


def users_user_id_delete(userId):  # noqa: E501
    """Deregister an user

     # noqa: E501

    :param user_id: User identifier
    :type user_id: str

    :rtype: None
    """
    s = base.check_session()
    u = _finduser(userId)
    k = u['username']
    if k == s['user']['username']:
        flask.abort(500)
    del users[userId]


def users_user_id_get(userId):  # noqa: E501
    """List details of specific user

     # noqa: E501

    :param user_id: User identifier
    :type user_id: str

    :rtype: User
    """
    base.check_session()
    return _cleanuser(_finduser(userId))


def users_user_id_put(userId, body):  # noqa: E501
    """Update user information

     # noqa: E501

    :param user_id: User identifier
    :type user_id: str
    :param user: Update user object
    :type user: dict | bytes

    :rtype: User
    """
    base.check_session()
    u = _finduser(userId)
    for k, v in body.items():
        if k not in ['id', 'created', 'createdBy'] and v is not None:
            u[k] = v
    return _cleanuser(u)
