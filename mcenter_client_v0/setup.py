from setuptools import setup, find_packages

version = "v0"
name = "mcenter_client_v0"
release = version
desc = "MCenter python client code"
lic = "ParallelM"

required_python_pkgs = ["requests"]

setup(
    name=name,
    namespace_packages=['parallelm'],
    version=release,
    description=desc,
    license=lic,
    zip_safe=False,
    install_requires=required_python_pkgs,
    packages=find_packages(exclude=["build", "*.tests", "*.tests.*", "tests.*", "tests"]),
)

