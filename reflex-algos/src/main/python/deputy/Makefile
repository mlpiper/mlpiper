
DEPUTYDISTDIR = ""

DEPUTY_EGG_PY2_SYMLINK = "deputy-py2.egg"
DEPUTY_EGG_PY3_SYMLINK = "deputy-py3.egg"

DEPUTY_EGG_PY2 = $(shell realpath $(shell ls ./dist/*-py2.*.egg))
DEPUTY_EGG_PY3 = $(shell realpath $(shell ls ./dist/*-py3.*.egg))
DEPUTY_EGG_PY2_BASENAME = $(shell basename $(DEPUTY_EGG_PY2))
DEPUTY_EGG_PY3_BASENAME = $(shell basename $(DEPUTY_EGG_PY3))

egg:
	rm -rf ./dist
	python2 setup.py bdist_egg
	python3 setup.py bdist_egg

install_to: egg
ifneq ($(DEPUTYDISTDIR),"")
	mkdir -p $(DEPUTYDISTDIR)
	cp -f $(DEPUTY_EGG_PY2) $(DEPUTY_EGG_PY3) $(DEPUTYDISTDIR)
	cd $(DEPUTYDISTDIR); ln -sf $(DEPUTY_EGG_PY2_BASENAME) $(DEPUTY_EGG_PY2_SYMLINK); cd -
	cd $(DEPUTYDISTDIR); ln -sf $(DEPUTY_EGG_PY3_BASENAME) $(DEPUTY_EGG_PY3_SYMLINK); cd -
endif


bdist:
	python setup.py bdist


test: egg
	env PYTHONPATH=./:tests/ python2 -m py.test -v tests/
	env PYTHONPATH=./:tests/ python3 -m pytest -v tests/

local: egg
	cp  dist/*.egg  ../../../../target/

run: egg
	./bin/run-deputy.sh

clean:
	rm -rf dist
	rm -rf build
