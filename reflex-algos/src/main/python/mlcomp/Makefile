# It make sense also to move all the component documentation to be done via python sphinx

MLCOMPDISTDIR = ""

MLCOMP_EGG_PY2_SYMLINK = "mlcomp-py2.egg"
MLCOMP_EGG_PY3_SYMLINK = "mlcomp-py3.egg"

bdist:
	python setup.py bdist

egg:
	rm -rf ./dist
	python2 setup.py -q bdist_egg
	python3 setup.py -q bdist_egg

MLCOMP_EGG_PY2 = $(shell realpath $(shell ls ./dist/*-py2.*.egg))
MLCOMP_EGG_PY3 = $(shell realpath $(shell ls ./dist/*-py3.*.egg))
MLCOMP_EGG_PY2_BASENAME = $(shell basename $(MLCOMP_EGG_PY2))
MLCOMP_EGG_PY3_BASENAME = $(shell basename $(MLCOMP_EGG_PY3))

install_to: egg
ifneq ($(MLCOMPDISTDIR),"")
	mkdir -p $(MLCOMPDISTDIR)
	cp -f $(MLCOMP_EGG_PY2) $(MLCOMP_EGG_PY3) $(MLCOMPDISTDIR)
	cd $(MLCOMPDISTDIR); ln -sf $(MLCOMP_EGG_PY2_BASENAME) $(MLCOMP_EGG_PY2_SYMLINK); cd -
	cd $(MLCOMPDISTDIR); ln -sf $(MLCOMP_EGG_PY3_BASENAME) $(MLCOMP_EGG_PY3_SYMLINK); cd -
endif


wheel:
	python2 setup.py sdist bdist_wheel
	python3 setup.py sdist bdist_wheel

# Creating 2 eggs one for python components, second for pyspark components
python_comps_egg:
	rm -rf /tmp/all-python-comp
	./bin/create-all-python-comp-dir.sh /tmp/all-python-comp
	./bin/create-components-egg.sh --components-dir /tmp/all-python-comp --dst-dir /tmp/
	cd /tmp/; for file in mcenter_components-*; do mv -v $$file python_$${file}; done; cd -

pyspark_comps_egg:
	./bin/create-components-egg.sh --components-dir ../../../../components/PySpark/ --dst-dir /tmp/
	cd /tmp/; for file in mcenter_components-*; do mv -v $$file pyspark_$${file}; done; cd -

comp_eggs: python_comps_egg pyspark_comps_egg

mlops_egg:
	cd ../mlops ; make egg; cd -

MLCOMP_EGG_PY2 = $(shell realpath $(shell ls dist/*-py2.*.egg))
MLCOMP_EGG_PY3 = $(shell realpath $(shell ls dist/*-py3.*.egg))
MLOPS_EGG_PY2 = $(shell realpath $(shell ls ../mlops/dist/*-py2.*.egg))
MLOPS_EGG_PY3 = $(shell realpath $(shell ls ../mlops/dist/*-py3.*.egg))
COMP_EGG_PY2 = $(shell realpath $(shell ls /tmp/python_mcenter_components*-py2.*.egg))
COMP_EGG_PY3 = $(shell realpath $(shell ls /tmp/python_mcenter_components*-py3.*.egg))
SPARK_JARS = "../../../../target/ReflexAlgos-jar-with-dependencies.jar"

test: egg mlops_egg comp_eggs
	# Running the tests with the python egg
	env PYTHONPATH=$(MLCOMP_EGG_PY2):$(MLOPS_EGG_PY2):$(COMP_EGG_PY2):tests SPARK_JARS=$(SPARK_JARS) python2 -m py.test tests/
	env PYTHONPATH=$(MLCOMP_EGG_PY3):$(MLOPS_EGG_PY3):$(COMP_EGG_PY3):tests SPARK_JARS=$(SPARK_JARS) python3 -m pytest tests/

	# TODO: add running tests for pyspark components

local: test
	cp  dist/*.egg  ../../../../target/


install: wheel
	env PYTHONPATH=./ bin/install_locally.py

clean:
	\rm -rf dist
	\rm -rf build
