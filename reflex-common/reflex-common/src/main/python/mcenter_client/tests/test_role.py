import sys

sys.path.append('..')

import pytest
import parallelm.mcenter_client as mcenter_client


def test_lifecycle(server_auth):
    for u in server_auth.role.list():
        if u.name in ('test role', 'test role 2'):
            u.delete()
    created = server_auth.role.create(dict(name='test role',
                                           description='Test Role Desc'))
    assert created.name == 'test role'
    assert created.description == 'Test Role Desc'
    assert created.id is not None
    assert created.created >= 0
    assert created.created_by == 'admin'
    cnt = 0
    for u in server_auth.role.list():
        if u.name == 'test role':
            cnt += 1
            assert u.description == created.description
            assert u.id == created.id
            # created timestamp changes on real server
            # assert u.created == created.created
            assert u.created_by == created.created_by
    assert cnt == 1

    u = server_auth.role.get(created.id)
    assert u.name == created.name
    assert u.description == created.description
    assert u.id == created.id
    # created timestamp changes on real server
    # assert u.created == created.created
    assert u.created_by == created.created_by

    created.update(name='test role 2')
    assert created.name == 'test role 2'
    assert created.description == u.description
    assert created.id == u.id
    # created timestamp changes on real server
    # assert created.created == u.created
    assert created.created_by == u.created_by

    created.delete()
    for w in server_auth.role.list():
        assert w.name != 'test role'
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_auth.role.delete(u.id)
    assert excinfo.value.status in (500, 404)
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_auth.role.get(u.id)
    assert excinfo.value.status in (500, 404)


def test_agent_not_logged_in(server_connection):
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_connection.role.list()
    assert excinfo.value.status in (500, 401)
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_connection.role.create(dict(name='test role',
                                           description='test desc'))
    assert excinfo.value.status in (500, 401)
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_connection.role.get('test_id')
    assert excinfo.value.status in (500, 401)
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_connection.role.delete('test_id')
    assert excinfo.value.status in (500, 401)
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_connection.role.update('test_id',
                                      dict(name='test role',
                                           description='test desc'))
    assert excinfo.value.status in (500, 401)
