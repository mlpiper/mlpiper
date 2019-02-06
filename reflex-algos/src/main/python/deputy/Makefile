

egg:
	python setup.py bdist_egg


bdist:
	python setup.py bdist


test: egg
	env PYTHONPATH=./:tests/ python2 -m py.test -v tests/
	env PYTHONPATH=./:tests/ python3 -m pytest -v tests/


local: egg
	cp  dist/*.egg  ../../../../target/


run: egg
	./bin/run-deputy.sh
