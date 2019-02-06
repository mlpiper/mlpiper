from setuptools import setup, find_packages

setup(
    name="mcenter_client",
    namespace_packages=['parallelm'],
    version="1.0",
    description="MCenter Client APIs",
    license="ParallelM",
    zip_safe=True,
    include_package_data=True,
    exclude_package_data={'': ['*test*']},
    package_data={'': []},
    packages=find_packages('.',
                           exclude=[]),
    data_files=[('.', ['setup.py'])]
)
