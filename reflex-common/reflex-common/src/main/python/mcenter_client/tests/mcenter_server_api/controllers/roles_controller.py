import connexion
import six
import flask

from mcenter_server_api.models.role import Role  # noqa: E501
from mcenter_server_api import util

from . import base

roles = dict()


def roles_get():  # noqa: E501
    """Get list of roles

     # noqa: E501


    :rtype: List[Role]
    """
    base.check_session()
    return list(roles.values())


def roles_post(body):  # noqa: E501
    """Register an role

     # noqa: E501

    :param role: Role detail description
    :type role: dict | bytes

    :rtype: Role
    """
    s = base.check_session()
    if not body.get('name', None) or not body.get('description', None):
        flask.abort(500)
    base.finish_creation(body, s, roles)
    return body


def roles_role_id_delete(roleId):  # noqa: E501
    """Deregister an role

     # noqa: E501

    :param role_id: Role identifier
    :type role_id: str

    :rtype: None
    """
    base.check_session()
    if roleId not in roles:
        flask.abort(404)
    del roles[roleId]


def roles_role_id_get(roleId):  # noqa: E501
    """List details of specific role

     # noqa: E501

    :param role_id: Role identifier
    :type role_id: str

    :rtype: Role
    """
    base.check_session()
    if roleId not in roles:
        flask.abort(404)
    return roles[roleId]


def roles_role_id_put(roleId, body):  # noqa: E501
    """Update role information

     # noqa: E501

    :param role_id: Role identifier
    :type role_id: str
    :param role: Update role object
    :type role: dict | bytes

    :rtype: Role
    """
    base.check_session()
    if roleId not in roles:
        flask.abort(404)
    role = roles[roleId]
    for k, v in body.items():
        if k == 'name':
            # Check for name duplicates?
            role[k] = v
        elif k in ['id', 'created', 'createdBy']:
            pass  # These are read only
        else:
            role[k] = v
    return role
