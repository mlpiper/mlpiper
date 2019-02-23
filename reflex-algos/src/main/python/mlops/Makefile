# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = python3 -msphinx
SPHINXPROJ    = ParallelM
SOURCEDIR     = doc-src
BUILDDIR      = build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile bdist

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)


bdist:
	python setup.py bdist

egg:
	python2 setup.py bdist_egg
	python3 setup.py bdist_egg

wheel:
	python2 setup.py bdist_wheel
	python3 setup.py bdist_wheel

test: egg
	env PYTHONPATH=./:tests/:${SPARK_HOME}/python python2 -m py.test tests/
	env PYTHONPATH=./:tests/:${SPARK_HOME}/python python3 -m pytest tests/

local: test
	cp dist/parallelm-1.0.1-py2.7.egg ../../../../target/
	cp dist/parallelm-1.0.1-py3.*.egg ../../../../target/

cover: egg
	env PYTHONPATH=./ coverage2 run -m py.test tests/
	coverage2 report --include "parallelm/mlops/*"

# The target test can be used to run a simple example directly.
# Please define the environment variable SPARK_SUBMIT to point to a spark-submit binary
test_pyspark: egg
	env PM_MLOPS_MODE=pyspark \
		PM_REST_SERVER_WAIT_FOR_EXIT=0 \
		${SPARK_SUBMIT} \
				  --master spark://localhost:7077 \
				  --py-files dist/parallelm-1.0.0-py2.7.egg \
				  --jars ../../../../target/ReflexAlgos-jar-with-dependencies.jar \
				  examples/pi_calc.py

test_tf: egg
	env PM_MLOPS_MODE=tensorflow \
		PM_REST_SERVER_WAIT_FOR_EXIT=0 \
		PM_MLOPS_FILE="/tmp/tf_mlops.channel" \
		${SPARK_SUBMIT} \
				  --master spark://localhost:7077 \
				  --py-files dist/parallelm-1.0.0-py2.7.egg \
				  --jars ../../../../target/ReflexAlgos-jar-with-dependencies.jar \
				  examples/pi_calc.py

test_python: egg
	env PM_MLOPS_MODE=python \
		PM_MLOPS_FILE="/tmp/mlops.channel" \
		PYTHONPATH=dist/parallelm-1.0.0-py2.7.egg \
		python2 examples/mlops_example_python.py
	@echo "========"
	cat /tmp/mlops.channel

test_non_pm: egg
	env PM_MLOPS_FILE="/tmp/mlops.csv"\
	${SPARK_SUBMIT} \
				  --master spark://localhost:7077 \
				  --py-files dist/parallelm-1.0.0-py2.7.egg \
				  --jars ../../../../target/ReflexAlgos-jar-with-dependencies.jar \
				  examples/pi_calc.py

install: wheel
	env PYTHONPATH=./ bin/install_locally.py

clean:
	\rm -rf dist
	\rm -rf build
