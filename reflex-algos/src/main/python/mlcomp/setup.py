from setuptools import setup, find_packages

from parallelm.mlcomp import version, project_name

setup(
    name=project_name,
    namespace_packages=['parallelm'],
    version=version,
    description="An engine for running component based ML pipelines",
    license="ParallelM",
    zip_safe=False,
    include_package_data=True,
    package_data={'': ['*.json', '*.jar', '*.egg']},
    packages=find_packages('.'),
    data_files=[('.', ['__main__.py', 'setup.py'])],
    entry_points={
        'setuptools.installation': [
            'eggsecutable = parallelm.main:main'
        ]
    },
    scripts=["bin/mlpiper",
             "bin/mcenter_components_setup.py",
             "bin/deployment-runner.sh",
             "bin/create-egg.sh"],
    install_requires=[
        'termcolor',
        'flask'
    ],
)
