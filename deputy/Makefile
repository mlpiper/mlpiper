
DISTDIR = ""

DEPUTY_EGG_PY2_SYMLINK = "deputy-py2.egg"
DEPUTY_EGG_PY3_SYMLINK = "deputy-py3.egg"

DEPUTY_EGG_PY2 = $(shell realpath $(shell ls ./dist/*-py2.*.egg))
DEPUTY_EGG_PY3 = $(shell realpath $(shell ls ./dist/*-py3.*.egg))
DEPUTY_EGG_PY2_BASENAME = $(shell basename $(DEPUTY_EGG_PY2))
DEPUTY_EGG_PY3_BASENAME = $(shell basename $(DEPUTY_EGG_PY3))

egg local: _dist
	cd dist; ln -sf $(DEPUTY_EGG_PY2_BASENAME) $(DEPUTY_EGG_PY2_SYMLINK); cd -
	cd dist; ln -sf $(DEPUTY_EGG_PY3_BASENAME) $(DEPUTY_EGG_PY3_SYMLINK); cd -

_dist:
	rm -rf ./dist
	python2 setup.py -q bdist_egg
	python3 setup.py -q bdist_egg


install_to: egg
ifneq ($(DISTDIR),"")
	mkdir -p $(DISTDIR)
	cp -f $(DEPUTY_EGG_PY2) $(DEPUTY_EGG_PY3) $(DISTDIR)
	cd $(DISTDIR); ln -sf $(DEPUTY_EGG_PY2_BASENAME) $(DEPUTY_EGG_PY2_SYMLINK); cd -
	cd $(DISTDIR); ln -sf $(DEPUTY_EGG_PY3_BASENAME) $(DEPUTY_EGG_PY3_SYMLINK); cd -
endif


bdist:
	python setup.py bdist


test: egg
	env PYTHONPATH=./:tests/ python2 -m py.test -v tests/
	env PYTHONPATH=./:tests/ python3 -m pytest -v tests/

run: egg
	./bin/run-deputy.sh

clean:
	rm -rf dist
	rm -rf build
