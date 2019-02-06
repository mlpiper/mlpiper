import sys

sys.path.append('..')

import pytest
import parallelm.mcenter_client as mcenter_client


def test_group_lifecycle(server_auth):
    for group in server_auth.group.list():
        if group.name in ['test_group', 'test_group2']:
            group.delete()
    group = server_auth.group.create(dict(name='test_group'))
    assert group.name == 'test_group'
    assert group.id is not None
    assert group.created_by == 'admin'
    assert group.created > 0
    cnt = 0
    for a in server_auth.group.list():
        if a.name == group.name:
            cnt += 1
            assert a.id == group.id
            assert a.created == group.created
            assert a.created_by == group.created_by
    assert cnt == 1
    a = group.update(dict(name='test_group2'))
    assert a.name == 'test_group2'
    assert a.id == group.id
    assert a.created == group.created
    assert a.created_by == group.created_by
    b = server_auth.group.get(group.id)
    assert a.name == b.name
    assert a.id == b.id
    assert a.created == b.created
    assert a.created_by == b.created_by
    group.delete()
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        b.delete()
    assert excinfo.value.status == 404


def test_group_not_logged_in(server_connection):
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_connection.group.list()
    assert excinfo.value.status in (500, 401)
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_connection.group.create(dict(name='test_group'))
    assert excinfo.value.status in (500, 401)
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_connection.group.get('test_id')
    assert excinfo.value.status in (500, 401)
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_connection.group.delete('test_id')
    assert excinfo.value.status in (500, 401)
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_connection.group.update('test_id', dict(name='test_group'))
    assert excinfo.value.status in (500, 401)
