from setuptools import setup, find_packages

setup(
    name="mlcomp",
    namespace_packages=['parallelm'],
    version="1.0",
    description="MLOps PySpark engine to execute machine learning pipelines",
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
    }
)
