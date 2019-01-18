# It make sense also to move all the component documentation to be done via python sphinx

bdist:
	python setup.py bdist

egg:
	./bin/create-egg.sh

wheel:
	python2 setup.py bdist_wheel
	python3 setup.py bdist_wheel

python_comps_egg:
	rm -rf /tmp/all-python-comp
	./bin/create-all-python-comp-dir.sh /tmp/all-python-comp
	./bin/create-components-egg.sh --components-dir /tmp/all-python-comp --dst-dir /tmp/
	mv /tmp/mcenter_components-0.1-py2.7.egg /tmp/python_mcenter_components-0.1-py2.7.egg

pyspark_comps_egg:
	# Creating 2 eggs one for python components, second for pyspark components
	./bin/create-components-egg.sh --components-dir ../../../../components/PySpark/ --dst-dir /tmp/
	mv /tmp/mcenter_components-0.1-py2.7.egg /tmp/pyspark_mcenter_components-0.1-py2.7.egg

comp_eggs: python_comps_egg pyspark_comps_egg

mlops_egg:
	cd ../mlops ; make egg

MLCOMP_EGG := $(shell realpath dist/mlcomp-1.0-py2.7.egg)
MLOPS_EGG := $(shell realpath ../mlops/dist/parallelm-1.0.1-py2.7.egg)
COMP_EGG := "/tmp/python_mcenter_components-0.1-py2.7.egg"
SPARK_JARS := "../../../../target/ReflexAlgos-jar-with-dependencies.jar"

test: egg comp_eggs

	# Running the tests with the python egg
	env PYTHONPATH=$(MLCOMP_EGG):$(MLOPS_EGG):$(COMP_EGG):tests SPARK_JARS=$(SPARK_JARS) python2 -m py.test tests/
	env PYTHONPATH=$(MLCOMP_EGG):$(MLOPS_EGG):$(COMP_EGG):tests SPARK_JARS=$(SPARK_JARS) python3 -m pytest tests/

	# TODO: add running tests for pyspark components

local: test
	cp  dist/*.egg  ../../../../target/


install: wheel
	env PYTHONPATH=./ bin/install_locally.py

clean:
	\rm -rf dist
	\rm -rf build