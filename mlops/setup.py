import os
from setuptools import setup, find_packages

import protobuf_dep
protobuf_dep.copy_protobuf_sources()

from parallelm.mlops.constants import Constants

# This import should be placed after the copying of protobuf python sources

# The directory containing this file
ROOT = os.path.dirname(os.path.abspath(__file__))

# The text of the README file
README = open(ROOT + "/README.md").read()

setup(
    name=Constants.OFFICIAL_NAME,
    namespace_packages=['parallelm'],
    version=Constants.MLOPS_CURRENT_VERSION,
    description="A library to read and report MLApp statistics",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/mlpiper/mlpiper/tree/master/reflex-algos/src/main/python/mlops",
    author="ParallelM",
    author_email="info@parallelm.com",
    license="Apache License 2.0",
    classifiers=[
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS",
        "Operating System :: POSIX",
        "Operating System :: Unix",
    ],
    zip_safe=False,
    include_package_data=True,
    package_data={'': ['*.json', '*.jar', '*.egg']},
    packages=find_packages(exclude=["build", "*.tests", "*.tests.*", "tests.*", "tests"]),
    install_requires=[
        "pandas",
        "termcolor",
        "kazoo",
        "protobuf",
        "requests",
        "py4j"
    ],
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*',
    entry_points={
    }
)
