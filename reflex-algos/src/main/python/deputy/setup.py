from setuptools import setup, find_packages


name = "deputy"
release = "1.0"
desc = "Deputy program to run inside containers"
lic = "ParallelM"

required_python_pkgs = []

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

    data_files=[('.', ['__main__.py', 'setup.py'])],
    entry_points={
        'setuptools.installation': [
            'eggsecutable = parallelm.deputy.main:main'
        ]
    }
)
