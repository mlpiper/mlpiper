from setuptools import setup, find_packages

setup(
    name="mcenter-components",
    namespace_packages=['parallelm'],
    version="0.1",
    description="MCenter components",
    license="ParallelM",
    zip_safe=False,
    include_package_data=True,
    package_data={'': ['*.json', '*.jar', '*.egg', '*.txt', "*.R", "*.r", "*.ipynb"]},
    packages=find_packages('.'),
    data_files=[('.', ['setup.py'])]
)
