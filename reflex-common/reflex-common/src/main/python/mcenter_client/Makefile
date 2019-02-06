DISTDIR := dist

SOURCE_YAML_DIR := ../../../rest-apis
SOURCE_YAML := MCenterRestApi.yaml

FLAT_JSON := build/MCenterRestApi-flat.json
RESTAPI_DIR := parallelm/mcenter_client/rest_api
CLIENT_API := $(RESTAPI_DIR)/api_client.py
DUMMY_SERVER_DIR := build/dummyserver/mcenter_server_api
DUMMY_SERVER_API := $(DUMMY_SERVER_DIR)/openapi/openapi.yaml
MOCK_SERVER_DIR := tests/mcenter_server_api
MOCK_SERVER_API := $(MOCK_SERVER_DIR)/openapi/openapi.yaml
HTML_FILE := build/html/index.html
HTML_PUBLISHED_FILE := $(DISTDIR)/mcenter_api.html

OPENAPI_GENERATOR_URL := http://central.maven.org/maven2/org/openapitools/openapi-generator-cli/3.3.1/openapi-generator-cli-3.3.1.jar
OPENAPI_GENERATOR_FILE := build/openapi-generator-cli.jar

OPENAPI_GENERATOR := docker run --user=$$UID:$$GID --rm -v `pwd`:/local -w /local openapitools/openapi-generator-cli
OPENAPI_GENERATOR := java -jar $(OPENAPI_GENERATOR_FILE)
	
bdist: $(CLIENT_API)
	python3 setup.py bdist

wheel: wheel2 wheel3

wheel2: $(CLIENT_API)
	python2 setup.py bdist_wheel --dist-dir $(DISTDIR)

wheel3: $(CLIENT_API)
	python3 setup.py bdist_wheel --dist-dir $(DISTDIR)

egg: egg2 egg3

egg2: $(CLIENT_API)
	python2 setup.py bdist_egg --dist-dir $(DISTDIR)

egg3: $(CLIENT_API)
	python3 setup.py bdist_egg --dist-dir $(DISTDIR)

test: test2 test3

test2: $(CLIENT_API) $(MOCK_SERVER_API)
	python2 -m py.test tests/

test3: $(CLIENT_API) $(MOCK_SERVER_API)
	python3 -m pytest tests/

local: test egg wheel
	cp -t ../../../../target dist/*.egg dist/*.whl

clean:
	rm -rf build dist *.egg-info .cache .pytest_cache $(RESTAPI_DIR)
	find . -depth -name __pycache__ -type d -exec rm -rf {} \;
	find . -name '*.pyc' -type f -delete
	
all: api

testx: tests/test.py api
	cd tests && python3 ./test.py

api: $(CLIENT_API) $(DUMMY_SERVER_API) $(MOCK_SERVER_API)

$(CLIENT_API): build/rest_api/mcenter_rest_api/api_client.py
	mkdir -p $(RESTAPI_DIR)/api $(RESTAPI_DIR)/models
	for f in `cd build/rest_api/mcenter_rest_api && find . -maxdepth 1 -name '*.py'`; do \
		sed -e's/import mcenter_rest_api.models/from . import models/' \
	            -e's/ mcenter_rest_api\./ ./' \
                    -e's/ mcenter_rest_api / . /' \
                    -e's/"mcenter_rest_api"/"mcenter_client"/' \
                    -e's/mcenter_rest_api.models/models/' \
	            < build/rest_api/mcenter_rest_api/$$f > $(RESTAPI_DIR)/$$f; \
	done
	for f in `cd build/rest_api/mcenter_rest_api/api && find . -maxdepth 1 -name '*.py'`; do \
		sed -e's/ mcenter_rest_api.api\./ ./' \
		    -e's/ mcenter_rest_api.api_client / ..api_client /' \
	            < build/rest_api/mcenter_rest_api/api/$$f > $(RESTAPI_DIR)/api/$$f; \
	done
	for f in `cd build/rest_api/mcenter_rest_api/models && find . -maxdepth 1 -name '*.py'`; do \
		sed -e's/ mcenter_rest_api.models\./ ./' \
		    < build/rest_api/mcenter_rest_api/models/$$f > $(RESTAPI_DIR)/models/$$f; \
	done

build/rest_api/mcenter_rest_api/api_client.py: $(FLAT_JSON) Makefile $(OPENAPI_GENERATOR_FILE) | build/rest_api
	$(OPENAPI_GENERATOR) generate \
		-i $< \
		-g python \
		-o build/rest_api \
		-c config/rest_api.json

$(MOCK_SERVER_API): $(DUMMY_SERVER_API)
	cp -rfvu -t $(MOCK_SERVER_DIR) $(DUMMY_SERVER_DIR)/models $(DUMMY_SERVER_DIR)/*.py
	sed -e 's/^security://' -e 's/^- token: \[\]//' < "$<" > "$@"

$(DUMMY_SERVER_API): $(FLAT_JSON) Makefile $(OPENAPI_GENERATOR_FILE)
	$(OPENAPI_GENERATOR) generate \
		-i $< \
		-g python-flask \
		-o build/dummyserver \
		-c config/dummyserver.json

html: $(HTML_PUBLISHED_FILE)

$(HTML_PUBLISHED_FILE): $(HTML_FILE) | $(DISTDIR)
	cp -f $< $@

$(HTML_FILE): $(FLAT_JSON) $(OPENAPI_GENERATOR_FILE) Makefile
	$(OPENAPI_GENERATOR) generate \
		-i $< \
		-g html \
		-o build/html \
		-c config/html.json

build build/rest_api $(DISTDIR):
	mkdir -p $@

$(FLAT_JSON): $(SOURCE_YAML_DIR)/$(SOURCE_YAML) \
              $(shell find $(SOURCE_YAML_DIR) -name '*.yaml') \
              Makefile merge-openapi.py | build
	python3 merge-openapi.py $< > $@

$(OPENAPI_GENERATOR_FILE): | build
	curl -o $@ "$(OPENAPI_GENERATOR_URL)"

.PHONY: all test test2 test3 clean api bdist egg egg2 egg3 local html
