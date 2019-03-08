# It make sense also to move all the component documentation to be done via python sphinx


wheel:
	python setup.py bdist_wheel


test:
	# Running the tests with the python egg
	env PYTHONPATH=./ python3 -m pytest -s -vv tests/

install: wheel
	env PYTHONPATH=./ bin/install_locally.py

clean:
	\rm -rf dist
	\rm -rf build
