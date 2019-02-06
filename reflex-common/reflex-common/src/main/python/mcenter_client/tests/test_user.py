import sys

sys.path.append('..')

import pytest
import parallelm.mcenter_client as mcenter_client


def test_login(server_auth):
    pass


def test_me_logged_off(server_connection):
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        user = server_connection.user.me()
    assert excinfo.value.status in (500, 401)


def test_login_baduser(server_connection):
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_connection.login(dict(username='nonexistent user',
                                     password='password'))
    assert excinfo.value.status in (500, 403)


def test_login_badpassword(server_connection):
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_connection.login(dict(username='admin', password='badpassword'))
    assert excinfo.value.status in (500, 403)


def test_me(server_auth):
    user = server_auth.user.me()
    assert user.username == 'admin'
    assert user.password is None


def test_changepasswd(server_auth):
    user = server_auth.user.list()
    for u in server_auth.user.list():
        if u.username == 'admin':
            u.update(dict(password='admin'))


def test_lifecycle(server_auth):
    for u in server_auth.user.list():
        if u.username == 'test user':
            u.delete()
    created = server_auth.user.create(dict(username='test user', password='pt',
                                           accountType='user',
                                           authMode="internal"))
    assert created.username == 'test user'
    assert created.id is not None
    assert created.password is None
    assert created.created != 0
    assert created.created_by == 'admin'
    cnt = 0
    for u in server_auth.user.list():
        if u.username == 'test user':
            cnt += 1
            assert u.id == created.id
            assert u.password is None
            assert u.created == created.created
            assert u.created_by == created.created_by
    assert cnt == 1

    u = server_auth.user.get(created.id)
    assert u.username == created.username
    assert u.id == created.id
    assert u.password is None
    assert u.created == created.created
    assert u.created_by == created.created_by

    testconn = mcenter_client.Client(
        configuration=server_auth._rest._client.configuration)
    testconn.login(dict(username='test user', password='pt'))

    created.update(dict(password='tp'))

    testconn.logout()

    with pytest.raises(mcenter_client.ApiException) as excinfo:
        testconn.login(dict(username='test user', password='pt'))
    assert excinfo.value.status == 403
    testconn.login(dict(username='test user', password='tp'))
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        testconn.user.delete(created.id)
    assert excinfo.value.status == 500

    created.delete()
    for w in server_auth.user.list():
        assert w.username != 'test user'
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_auth.user.delete(u.id)
    assert excinfo.value.status == 404
    with pytest.raises(mcenter_client.ApiException) as excinfo:
        server_auth.user.get(u.id)
    assert excinfo.value.status == 404
