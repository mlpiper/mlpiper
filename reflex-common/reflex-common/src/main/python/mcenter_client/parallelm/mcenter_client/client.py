#! /usr/bin/python3

from __future__ import print_function
import time

import collections
import copy
import sys
import traceback

from . import rest_api
from .rest_api.rest import ApiException


class _REST(object):
    def __init__(self, client):
        self._client = client
        self.token = None
        self.agent = rest_api.AgentsApi(client)
        self.default = rest_api.DefaultApi(client)
        self.group = rest_api.GroupsApi(client)
        self.ppatterns = rest_api.PipelinePatternsApi(client)
        self.pprofiles = rest_api.PipelineProfilesApi(client)
        self.role = rest_api.RolesApi(client)
        self.user = rest_api.UsersApi(client)

    def set_token(self, token):
        self.token = token
        self._client.cookie = 'token=' + token

    def clear_token(self):
        self.token = None
        self._client.cookie = None


class _Base(object):
    __slots__ = ("_factory", "_data", "id", "_nd", "_nest")

    def __init__(self, factory, data):
        object.__init__(self)
        self.id = data.id
        self._data = data
        self._factory = factory
        self._nd = None
        self._nest = 0

    def _getter(self, name):
        return getattr(self._data, name)

    def _setter(self, name, value):
        if self._nd is not None:
            setattr(self._nd, name, value)
        else:
            self.update({name: value})

    def _deleter(self, name):
        if self._nd is not None:
            delattr(self._nd, name)
        else:
            raise AttributeError("Cannot delete this attribute")

    def __enter__(self):
        if self._nest == 0:
            self._nd = self._copydata()
        self._nest += 1
        return self._nd

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._nest == 0:
            raise RuntimeError("__exit__ without __enter__")
        self._nest -= 1
        if self._nest == 0:
            try:
                self._data = self._update(self._nd)._data
            finally:
                self._nd = None
        return False

    def update(self, *args, **kwargs):
        with self as new_data:
            for d in args:
                for k, v in d.items():
                    setattr(new_data, k, v)
            for k, v in kwargs.items():
                setattr(new_data, k, v)
        return self

    def _update(self, data):
        raise NotImplementedError("Update is not supported")

    def delete(self):
        if self._nest != 0:
            raise RuntimeError("Cannot delete object in the middle of update")
        self._delete()
        self.id = None

    def _delete(self):
        raise NotImplementedError("Delete is not supported")

    def _copydata(self):
        return copy.copy(self._data)


def _populate(cls):
    for field in cls.RestType.openapi_types:
        if field != "id":
            f = "_%s" % field
            setattr(cls, field,
                    property(lambda self, f=f: self._getter(f),
                             lambda self, value, f=f: self._setter(f, value),
                             lambda self, f=f: self._deleter(f)))


class _BaseWithID(_Base):
    __slots__ = ()

    def _copydata(self):
        data = _Base._copydata(self)
        data.created = None
        data.created_by = None
        data.id = None
        return data

    def _update(self, data):
        return self._factory.update(self.id, data)

    def _delete(self):
        self._factory.delete(self.id)


class _BaseFactory(object):
    __slots__ = ("_rest")
    ClientType = None

    def __init__(self, rest):
        self._rest = rest

    def _get_id(self, id):
        if isinstance(id, (self.ClientType, self.ClientType.RestType)):
            return id.id
        if isinstance(id, collections.Mapping):
            return id['id']
        return id

    def _unwrap(self, obj):
        if isinstance(obj, self.ClientType):
            return obj._data
        return obj

    def _wrap(self, obj):
        return self.ClientType(self, obj)


class _User(_BaseWithID):
    __slots__ = ()

    RestType = rest_api.User

    def __str__(self):
        return "<User %s>" % self.id


_populate(_User)


class _UserFactory(_BaseFactory):
    __slots__ = ()
    ClientType = _User

    def __init__(self, rest):
        _BaseFactory.__init__(self, rest.user)

    def me(self):
        return self._wrap(self._rest.me_get())

    def list(self):
        return [self._wrap(u) for u in self._rest.users_get()]

    def create(self, user):
        return self._wrap(self._rest.users_post(self._unwrap(user)))

    def get(self, user_id):
        return self._wrap(self._rest.users_user_id_get(self._get_id(user_id)))

    def update(self, user_id, user):
        return self._wrap(self._rest.users_user_id_put(self._get_id(user_id),
                                                       self._unwrap(user)))

    def delete(self, user_id):
        return self._rest.users_user_id_delete(self._get_id(user_id))

    def validate(self, token):
        return self._rest.auth_validate_post("Bearer %s" % token)


class _Group(_BaseWithID):
    __slots__ = ()

    RestType = rest_api.Group

    def __str__(self):
        return "<Group %s>" % self.id


_populate(_Group)


class _GroupFactory(_BaseFactory):
    __slots__ = ()
    ClientType = _Group

    def __init__(self, rest):
        _BaseFactory.__init__(self, rest.group)

    def list(self):
        return [self._wrap(u) for u in self._rest.onboarding_groups_get()]

    def create(self, group):
        return self._wrap(
            self._rest.onboarding_groups_post(self._unwrap(group)))

    def get(self, group_id):
        return self._wrap(
            self._rest.onboarding_groups_group_id_get(self._get_id(group_id)))

    def update(self, group_id, group):
        return self._wrap(
            self._rest.onboarding_groups_group_id_put(self._get_id(group_id),
                                                      self._unwrap(group)))

    def delete(self, group_id):
        return self._rest.onboarding_groups_group_id_delete(
            self._get_id(group_id))


class _Agent(_BaseWithID):
    __slots__ = ()
    RestType = rest_api.Agent

    def __str__(self):
        return "<Agent %s>" % self.id


_populate(_Agent)


class _AgentFactory(_BaseFactory):
    __slots__ = ()
    ClientType = _Agent

    def __init__(self, rest):
        _BaseFactory.__init__(self, rest.agent)

    def list(self):
        return [self._wrap(u) for u in self._rest.agents_get()]

    def create(self, agent):
        return self._wrap(self._rest.agents_post(self._unwrap(agent)))

    def get(self, agent_id):
        return self._wrap(
            self._rest.agents_agent_id_get(self._get_id(agent_id)))

    def update(self, agent_id, agent):
        return self._wrap(
            self._rest.agents_agent_id_put(self._get_id(agent_id),
                                           self._unwrap(agent)))

    def delete(self, agent_id):
        self._rest.agents_agent_id_delete(self._get_id(agent_id))


class _Role(_BaseWithID):
    __slots__ = ()
    RestType = rest_api.Role

    def __str__(self):
        return "<Role %s>" % self.id


_populate(_Role)


class _RoleFactory(_BaseFactory):
    __slots__ = ()
    ClientType = _Role

    def __init__(self, rest):
        _BaseFactory.__init__(self, rest.role)

    def list(self):
        return [self._wrap(u) for u in self._rest.roles_get()]

    def create(self, role):
        return self._wrap(self._rest.roles_post(self._unwrap(role)))

    def get(self, role_id):
        return self._wrap(self._rest.roles_role_id_get(self._get_id(role_id)))

    def update(self, role_id, role):
        return self._wrap(self._rest.roles_role_id_put(self._get_id(role_id),
                                                       self._unwrap(role)))

    def delete(self, role_id):
        self._rest.roles_role_id_delete(self._get_id(role_id))


class _SettingsFactory(object):
    __slots__ = ("_rest")

    def __init__(self, rest):
        object.__init__(self)
        self._rest = rest.default

    def global_parameters(self):
        return self._rest.settings_global_parameters_get()

    def build_manifest(self):
        return self._rest.settings_build_manifest_get()


class Client(object):
    def __init__(self, server=None, user=None,
                 configuration=rest_api.Configuration()):
        if server is not None:
            if '//' in server:
                configuration.host = server
            elif ']:' in server or (':' in server and ']' not in server):
                configuration.host = "http://%s/v2" % server
            else:
                configuration.host = "http://%s:3456/v2" % server
        self._rest = _REST(rest_api.ApiClient(configuration))
        self.agent = _AgentFactory(self._rest)
        self.group = _GroupFactory(self._rest)
        self.role = _RoleFactory(self._rest)
        self.settings = _SettingsFactory(self._rest)
        self.user = _UserFactory(self._rest)
        if user is not None:
            self.login(user)

    def login(self, user):
        res = self._rest.user.auth_login_post(user)
        self._rest.set_token(res.token)

    def logout(self):
        self._rest.clear_token()

    def get_token(self):
        return self._rest.token
