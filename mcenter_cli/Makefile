DISTDIR := dist

.PHONY: help Makefile bdist wheel clean install install-client install-all client-tar build-client

MCENTER_CLIENT_DIST_DIR = "mcenter-cli-dist"
MCENTER_CLI_WHL = "./dist/mcenter_cli-1.2.3-py3-none-any.whl"
MCENTER_CLIENT_WHL = "../mcenter_client_v0/dist/mcenter_client_v0-0-py3-none-any.whl"


all: wheel

bdist:
	python setup.py bdist

pkgs: wheel egg

wheel: wheel2 wheel3

wheel2:
	python2 setup.py bdist_wheel --dist-dir $(DISTDIR)

wheel3:
	python3 setup.py bdist_wheel --dist-dir $(DISTDIR)

egg: egg2 egg3

egg2:
	python2 setup.py bdist_egg --dist-dir $(DISTDIR)

egg3:
	python3 setup.py bdist_egg --dist-dir $(DISTDIR)


clean:
	\rm -rf dist build

build-client:
	cd ../mcenter_client_v0/ && make && cd -

install-client: build-client
	. ~/venv/bin/activate && pip install -U ../mcenter_client_v0/dist/mcenter_client_v0-0-py3-none-any.whl

install: wheel
	. ~/venv/bin/activate && env PYTHONPATH=./ bin/install_locally.py

install-all: install-client install


client-tar: wheel build-client
	./build_client_tar.sh $(MCENTER_CLIENT_DIST_DIR) $(MCENTER_CLI_WHL) $(MCENTER_CLIENT_WHL)
