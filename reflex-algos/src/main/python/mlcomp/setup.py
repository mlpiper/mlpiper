from setuptools import setup, find_packages
import os
import sys
import shutil

from parallelm.mlcomp import project_name, normalized_package_name, shared_lib_rltv_dir, version

# The directory containing this file
ROOT = os.path.dirname(os.path.abspath(__file__))

# The text of the README file
README = open(ROOT + "/README.md").read()

install_requires = ['ml-ops', 'termcolor', 'flask', 'psutil', 'py4j']
if sys.version_info[0] < 3:
    install_requires.append('enum')


def create_shared_dir():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    shared_dir = os.path.join(script_dir, shared_lib_rltv_dir)
    if os.path.exists(shared_dir):
        shutil.rmtree(shared_dir)
    os.makedirs(shared_dir)

    src_mlcomp_jar_filepath = os.path.join(shared_dir, "..", "..", "..", "..", "..", "..", "..", "reflex-common",
                                           "mlcomp", "target", normalized_package_name + ".jar")
    shutil.copy(src_mlcomp_jar_filepath, shared_dir)


create_shared_dir()

setup(
    name=project_name,
    namespace_packages=['parallelm'],
    version=version,
    description="An engine for running component based ML pipelines",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/mlpiper/mlpiper/tree/master/reflex-algos/src/main/python/mlcomp",
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
    packages=find_packages(),
    data_files=[(shared_lib_rltv_dir, os.listdir(os.path.join(ROOT, shared_lib_rltv_dir)))],
    scripts=["bin/mlpiper",
             "bin/mcenter_components_setup.py",
             "bin/create-egg.sh",
             "bin/cleanup.sh"],
    install_requires=install_requires,
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*',
    entry_points={
        'setuptools.installation': [
            'eggsecutable = parallelm.mlpiper.main:main'
        ]
    }
)
