from setuptools import setup, find_packages
import os
import sys

from drycode import project_name, version

# The directory containing this file
ROOT = os.path.dirname(os.path.abspath(__file__))

# The text of the README file
README = open(ROOT + "/README.md").read()

install_requires = []
if sys.version_info[0] < 3:
    install_requires.append('enum')

setup(
    name=project_name,
    namespace_packages=['drycode'],
    version=version,
    description="An engine for generating definition files in multiple languages",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/mlpiper/mlpiper/tree/master/drycode",
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
    packages=find_packages('.'),
    # data_files=[('.', ['__main__.py', 'setup.py'])],
    scripts=["bin/drycode"],
    install_requires=install_requires,
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*',
    entry_points={
        'setuptools.installation': [
            'eggsecutable = drycode.main:main'
        ]
    }
)
