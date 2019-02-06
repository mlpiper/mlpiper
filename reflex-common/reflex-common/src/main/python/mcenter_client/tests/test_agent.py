import sys

sys.path.append('..')

import pytest
import parallelm.mcenter_client as mcenter_client


def test_agent_lifecycle(server_auth):
    for agent in server_auth.agent.list():
        if agent.address in ['test_agent', 'test_agent2']:
            agent.delete()
    agent = server_auth.agent.create(dict(address='test_agent'))
    assert agent.address == 'test_agent'
    assert agent.id is not None
    assert agent.created_by == 'admin'
    assert agent.created > 0
    cnt = 0
    for a in server_auth.agent.list():
        if a.address == agent.address:
            cnt += 1
            assert a.address == agent.address
            assert a.id == agent.id
            assert a.created == agent.created
            assert a.created_by == agent.created_by
    assert cnt == 1
    a = agent.update(dict(address='test_agent2'))
    assert a.address == 'test_agent2'
    assert a.id == agent.id
    assert a.created == agent.created
    assert a.created_by == agent.created_by
    b = server_auth.agent.get(agent.id)
    assert a.address == b.address
    assert a.id == b.id
    assert a.created == b.created
    assert a.created_by == b.created_by
    agent.delete()
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        b.delete()
    assert excinfo.value.status == 404


def test_agent_not_logged_in(server_connection):
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_connection.agent.list()
    assert excinfo.value.status in (500, 401)
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_connection.agent.create(dict(address='test_agent'))
    assert excinfo.value.status in (500, 401)
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_connection.agent.get('test_id')
    assert excinfo.value.status in (500, 401)
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_connection.agent.delete('test_id')
    assert excinfo.value.status in (500, 401)
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_connection.agent.update('test_id', dict(address='test_agent'))
    assert excinfo.value.status in (500, 401)
