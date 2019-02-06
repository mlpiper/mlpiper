from setuptools import setup, find_packages


from parallelm.mcenter_cli import version  # This import should be placed after the copying of protobuf python sources

name = "mcenter-cli"
release = version
desc = "MCenter command line tool"
lic = "ParallelM"

required_python_pkgs = ["requests"]

setup(
    name=name,
    namespace_packages=['parallelm'],
    version=release,
    description=desc,
    license=lic,
    zip_safe=False,
    scripts=["bin/mcenter-cli"],
    install_requires=required_python_pkgs,
    packages=find_packages(exclude=["build", "*.tests", "*.tests.*", "tests.*", "tests"]),
    py_modules=["__main__"],

)

