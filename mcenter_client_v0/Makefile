DISTDIR := dist

.PHONY: help Makefile bdist wheel clean

all: wheel egg

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
