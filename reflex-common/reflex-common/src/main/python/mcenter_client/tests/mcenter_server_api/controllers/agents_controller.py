import connexion
import six
import flask
import time
import os
import base64

from mcenter_server_api.models.agent import Agent  # noqa: E501
from mcenter_server_api import util

from . import base

agents = dict()


def _findagentbyaddr(addr):
    for agent in agents.values():
        if agent['address'] == addr:
            return agent
    return None


def agents_agent_id_delete(agentId):  # noqa: E501
    """Deregister an agent

     # noqa: E501

    :param agentId: Agent identifier
    :type agentId: str

    :rtype: None
    """
    base.check_session()
    if agentId not in agents:
        flask.abort(404)
    del agents[agentId]


def agents_agent_id_get(agentId):  # noqa: E501
    """List details of specific agent

     # noqa: E501

    :param agentId: Agent identifier
    :type agentId: str

    :rtype: Agent
    """
    base.check_session()
    if agentId not in agents:
        flask.abort(404)
    return agents[agentId]


def agents_agent_id_put(agentId, body):  # noqa: E501
    """Update agent information

     # noqa: E501

    :param agentId: Agent identifier
    :type agentId: str
    :param body: Update agent object
    :type body: dict | bytes

    :rtype: Agent
    """
    base.check_session()
    if agentId not in agents:
        flask.abort(404)
    agent = agents[agentId]
    for k, v in body.items():
        if k == 'address':
            a = _findagentbyaddr(v)
            if a is not None and a is not agent:
                flask.abort(500)
            agent[k] = v
        elif k in ['id', 'created', 'createdBy']:
            pass  # These are read only
        else:
            agent[k] = v
    return agent


def agents_get():  # noqa: E501
    """Get list of agents

     # noqa: E501


    :rtype: List[Agent]
    """
    base.check_session()
    return list(agents.values())


def agents_post(body):  # noqa: E501
    """Register an agent

     # noqa: E501

    :param agent: Agent address object
    :type agent: dict | bytes

    :rtype: Agent
    """
    s = base.check_session()
    a = body.get('address', None)
    if not a:
        flask.abort(500)
    if _findagentbyaddr(a) is not None:
        flask.abort(500)
    base.finish_creation(body, s, agents)
    return body
