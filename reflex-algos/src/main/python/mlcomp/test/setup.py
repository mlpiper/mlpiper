from setuptools import setup, find_packages

setup(
    name="mlops-components",
    namespace_packages=['parallelm'],
    version="0.1",
    description="MLOps PySpark components",
    license="ParallelM",
    zip_safe=False,
    include_package_data=True,
    package_data={'': ['*.json', '*.jar', '*.egg', '*.txt']},
    packages=find_packages('.'),
    data_files=[('.', ['setup.py'])]
)
