#! /usr/bin/python

import math
import random
import sys

import pytest

if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO


def genrows():
    return [[] for x in range(5001)]


def updaterows(rows, header, fn):
    i = -1
    for row in rows:
        if i == -1:
            row.append(header)
        else:
            row.append(str(fn(i)))
        i += 1


def save(rows):
    output = ""
    for row in rows:
        output = output + "\n" + ','.join(row)
    return StringIO(output)


@pytest.fixture(scope="module")
def genenrate_da():
    rows = genrows()
    updaterows(rows, 'Label', lambda i: i % 9)
    updaterows(rows, 'Zero', lambda i: '0.0')
    updaterows(rows, 'Ones', lambda i: 1)
    updaterows(rows, 'Twos', lambda i: 2)
    updaterows(rows, 'Sequence::Integer', lambda i: i)
    updaterows(rows, 'Triangle::Short', lambda i: i % 20)
    updaterows(rows, 'Triangle::Long', lambda i: int(i / 250))
    updaterows(rows, 'Random::Basic', lambda i: random.random())
    updaterows(rows, 'Random::100', lambda i: random.random() * 100)
    updaterows(rows, 'Random::PosNeg', lambda i: random.random() * 2 - 1)
    updaterows(rows, 'Frequent', lambda i: 7 if (i % 16) > 14 else i % 16)
    updaterows(rows, 'Random::015', lambda i: random.random() / 100.0 + 0.15)
    updaterows(rows, 'MeanMedian::Int', lambda i: 0 if i < 2501 else 100)
    updaterows(rows, 'MeanMedian::Float', lambda i: i / 2500.0 if i < 2501 else 5001 - i / 2500.0)
    updaterows(rows, 'Sine', lambda i: math.sin(i / 1000.0 * math.pi))
    updaterows(rows, 'Cosine', lambda i: math.cos(i / 1000.0 * math.pi))
    updaterows(rows, 'Square', lambda i: i * i)
    updaterows(rows, 'SquareRoot', lambda i: math.sqrt(i))
    updaterows(rows, 'Logarithm', lambda i: math.log1p(i))
    updaterows(rows, 'Sequence::Float', lambda i: i / 100.0)
    updaterows(rows, 'String::Mix', lambda i: '"22"' if (i % 2) != 0 else '"2"')
    updaterows(rows, 'String::Various', lambda i: '"' + 'x' * (i + 1) + '"')
    updaterows(rows, 'String::Fixed', lambda i: '"string"')

    return save(rows)


@pytest.fixture(scope="module")
def generate_da_with_missing_data():
    rows = genrows()
    updaterows(rows, 'Missing::All', lambda i: '')
    updaterows(rows, 'Missing::Even', lambda i: '' if (i % 2) != 0 else '0.0')
    updaterows(rows, 'Missing::Seq', lambda i: '' if i < 2500 else '0.0')
    updaterows(rows, 'Missing::Float', lambda i: '' if i < 2500 else i / 1000.0)
    updaterows(rows, 'emptystrings', lambda i: '""')
    return save(rows)


@pytest.fixture(scope="module")
def generate_da_categorical_data():
    rows = genrows()
    updaterows(rows, 'CategoricalOnly', lambda i: i % 20)
    return save(rows)


@pytest.fixture(scope="module")
def generate_da_continuous_data():
    rows = genrows()
    updaterows(rows, 'ContinuousOnly', lambda i: i)
    return save(rows)


@pytest.fixture(scope="session")
def spark_session(request):
    from pyspark.sql import SparkSession
    import os
    os.environ['PYSPARK_PYTHON'] = 'python2'
    if sys.version_info[0] >= 3:
        print("Setting Python 3")
        #  make sure pyspark tells workers to use python3 not 2 if both are installed
        os.environ['PYSPARK_PYTHON'] = 'python3'

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.jars", "../../../../target/ReflexAlgos-jar-with-dependencies.jar") \
        .appName("PyTest.PySparkUnitTests") \
        .getOrCreate()
    sc = spark.sparkContext
    request.addfinalizer(lambda: sc.stop())
    return spark
