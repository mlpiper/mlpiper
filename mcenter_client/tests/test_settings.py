import sys

sys.path.append('..')

import pytest
import parallelm.mcenter_client as mcenter_client


def test_global_parameters(server_auth):
    global_parameters = server_auth.settings.global_parameters()
    assert isinstance(global_parameters, dict)


def test_global_parameters_not_logged_in(server_connection):
    global_parameters = server_connection.settings.global_parameters()
    assert isinstance(global_parameters, dict)


def test_build_manifest(server_auth):
    manifest = server_auth.settings.build_manifest()
    assert isinstance(manifest, dict)


def test_build_manifest_not_logged_in(server_connection):
    manifest = server_connection.settings.build_manifest()
    assert isinstance(manifest, dict)
