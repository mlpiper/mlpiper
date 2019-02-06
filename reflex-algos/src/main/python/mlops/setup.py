from setuptools import setup, find_packages

import protobuf_dep
protobuf_dep.copy_protobuf_sources()

from parallelm.mlops import version  # This import should be placed after the copying of protobuf python sources

name = "parallelm"
release = version
desc = "mlops python library to interfance with the MLOps"
lic = "ParallelM"

required_python_pkgs = ["pandas", "kazoo"]

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

