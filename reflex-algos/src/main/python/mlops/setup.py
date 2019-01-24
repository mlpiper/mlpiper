from setuptools import setup, find_packages

import protobuf_dep
protobuf_dep.copy_protobuf_sources()

# This import should be placed after the copying of protobuf python sources
from parallelm.mlops import version, project_name

name = "parallelm"
release = version
desc = "mlops python library to interfance with the MLOps"
lic = "ParallelM"

required_python_pkgs = ["pandas", "kazoo", "termcolor"]

setup(
    name=name,
    namespace_packages=['parallelm'],
    version=release,
    description=desc,
    license=lic,
    zip_safe=False,
    scripts=[],
    install_requires=required_python_pkgs,
    packages=find_packages(exclude=["build", "*.tests", "*.tests.*", "tests.*", "tests"]),
)

