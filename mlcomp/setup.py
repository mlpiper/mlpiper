from setuptools import setup, find_packages
import os
import sys

from parallelm.mlcomp import project_name, version

# The directory containing this file
ROOT = os.path.dirname(os.path.abspath(__file__))

# The text of the README file
README = open(ROOT + "/README.md").read()

install_requires = ['ml-ops', 'wheel', 'termcolor', 'flask', 'flask_cors', 'psutil', 'py4j', 'nbformat', 'nbconvert',
                    'uwsgidecorators', 'sagemaker', 'pypsi', 'pytz', 'future']
if sys.version_info[0] < 3:
    install_requires.append('enum')

setup(
    name=project_name,
    version=version,
    description="An engine for running component based ML pipelines",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://mlpiper.github.io/mlpiper/mlcomp/",
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
