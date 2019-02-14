# It make sense also to move all the component documentation to be done via python sphinx

bdist:
	python setup.py bdist

egg:
	python2 setup.py -q bdist_egg
	python3 setup.py -q bdist_egg

wheel:
	python2 setup.py sdist bdist_wheel
	python3 setup.py sdist bdist_wheel

python_comps_egg:
	rm -rf /tmp/all-python-comp
	./bin/create-all-python-comp-dir.sh /tmp/all-python-comp
	./bin/create-components-egg.sh --components-dir /tmp/all-python-comp --dst-dir /tmp/
	rename 's/-py\d.\d//s' /tmp/mcenter_components-0.1-py*.egg
	mv /tmp/mcenter_components-0.1.egg /tmp/python_mcenter_components-0.1.egg

pyspark_comps_egg:
	# Creating 2 eggs one for python components, second for pyspark components
	./bin/create-components-egg.sh --components-dir ../../../../components/PySpark/ --dst-dir /tmp/
	rename 's/-py\d.\d//s' /tmp/mcenter_components-0.1-py*.egg
	mv /tmp/mcenter_components-0.1.egg /tmp/pyspark_mcenter_components-0.1.egg

comp_eggs: python_comps_egg pyspark_comps_egg

mlops_egg:
	cd ../mlops ; make egg; cd -

MLCOMP_EGG_PY2 = $(shell realpath $(shell ls dist/*-py2.*.egg))
MLCOMP_EGG_PY3 = $(shell realpath $(shell ls dist/*-py3.*.egg))
MLOPS_EGG_PY2 = $(shell realpath $(shell ls ../mlops/dist/*-py2.*.egg))
MLOPS_EGG_PY3 = $(shell realpath $(shell ls ../mlops/dist/*-py3.*.egg))
COMP_EGG= "/tmp/python_mcenter_components-0.1.egg"
SPARK_JARS = "../../../../target/ReflexAlgos-jar-with-dependencies.jar"

test: egg mlops_egg comp_eggs

	# Running the tests with the python egg
	env PYTHONPATH=$(MLCOMP_EGG_PY2):$(MLOPS_EGG_PY2):$(COMP_EGG):tests SPARK_JARS=$(SPARK_JARS) python2 -m py.test tests/
	env PYTHONPATH=$(MLCOMP_EGG_PY3):$(MLOPS_EGG_PY3):$(COMP_EGG):tests SPARK_JARS=$(SPARK_JARS) python3 -m pytest tests/

	# TODO: add running tests for pyspark components

local: test
	cp  dist/*.egg  ../../../../target/


install: wheel
	env PYTHONPATH=./ bin/install_locally.py

clean:
	\rm -rf dist
	\rm -rf build
