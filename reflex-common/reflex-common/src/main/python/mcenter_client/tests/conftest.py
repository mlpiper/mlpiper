import sys

sys.path.append('..')

import pytest
import parallelm.mcenter_client as mcenter_client
import threading
import time
import urllib3
import os.path


def pytest_addoption(parser):
    parser.addoption(
        '--server', action='store', default=None, help='Server name'
    )


def run_server():
    cmd = '''\
import connexion
from mcenter_server_api import encoder

app = connexion.FlaskApp(__name__,
                         specification_dir='mcenter_server_api/openapi',
                         debug=True)
app.app.json_encoder = encoder.JSONEncoder
app.add_api("openapi.yaml",
            arguments=dict(title="MLOps API documentation"))
app.run(port=13456, use_reloader=False)
'''
    if sys.version_info >= (3,):
        exec(cmd)
    else:
        import subprocess

        cmd = '''\
import threading
import os
import sys

def stop_on_eof():
    try:
        os.read(0, 1)
    except Exception as e:
        print("Failed %s" % e)
    os._exit(0)

t = threading.Thread(target=stop_on_eof)
t.start()
''' + cmd
        subprocess.call(['python3', '-c', cmd],
                        stdin=subprocess.PIPE,
                        cwd=os.path.dirname(__file__))


def start_server():
    server = threading.Thread(target=run_server)
    server.daemon = True
    server.start()
    http = urllib3.PoolManager()
    for retries in range(100):
        ok = True
        try:
            r = http.request('GET', 'http://localhost:13456/', retries=False)
            sys.stdout.write("Server ready after %u iterations\n" % retries)
            break
        except urllib3.exceptions.HTTPError as exc:
            ok = False
            time.sleep(0.05)
    else:
        http.request('GET', 'http://localhost:13456/', retries=False)

server_running = False


class lazy_server_connection:
    def __init__(self, server, user=None):
        self._server = server
        self._user = user
        self._client = None

    def __getattr__(self, attr):
        if self._client is None:
            if self._server is None:
                global server_running

                self._server = 'localhost:13456'
                if not server_running:
                    server_running = True
                    start_server()
            self._client = mcenter_client.Client(server=self._server, user=self._user)
        val = getattr(self._client, attr)
        setattr(self, attr, val)
        return val


@pytest.fixture
def server_connection(request):
    return lazy_server_connection(request.config.getoption('--server'))


@pytest.fixture
def server_auth(request):
    return lazy_server_connection(request.config.getoption('--server'),
                                  dict(username='admin', password='admin'))
