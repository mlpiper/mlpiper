from setuptools import setup, find_packages
from parallelm.deputy import package_name, version


desc = "Deputy program to run inside containers"
lic = "ParallelM"

required_python_pkgs = []

setup(
    name=package_name,
    namespace_packages=['parallelm'],
    version=version,
    description=desc,
    license=lic,
    zip_safe=False,
    scripts=[],
    install_requires=required_python_pkgs,
    packages=find_packages(exclude=["build", "*.tests", "*.tests.*", "tests.*", "tests"]),

    data_files=[('.', ['__main__.py', 'setup.py'])],
    entry_points={
        'setuptools.installation': [
            'eggsecutable = parallelm.deputy.main:main'
        ]
    }
)
