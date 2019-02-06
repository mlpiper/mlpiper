import connexion
import flask
import os
import base64
import time

_sessions = {}


def get_token():
    return connexion.request.cookies.get('token', None)


def generate_random(length=8):
    return base64.b32encode(os.urandom(length)).decode('ascii')


def finish_creation(d, session, owner):
    id = generate_random(8)
    while id in owner:
        id = generate_random(8)
    d['id'] = id
    d['created'] = time.time()
    d['createdBy'] = session['user']['username']
    owner[id] = d


def check_session():
    tok = get_token()
    conn = _sessions.get(tok, None)
    if conn is None:
        flask.abort(401)
    return conn


def add_session(user):
    token = generate_random(16)
    _sessions[token] = dict(user=user)
    return token
