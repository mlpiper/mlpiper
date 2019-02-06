# -*- coding: utf-8 -*-
import pickle

import pandas as pd
import sys

from pyspark.sql import SparkSession
from parallelm.mlops.mlops_mode import MLOpsMode
from parallelm.mlops.mlhealth import MLHealth
from parallelm.mlops import mlops

spark = SparkSession \
    .builder \
    .appName("SampleHealth") \
    .getOrCreate()

sc = spark.sparkContext

mlhealth = MLHealth()

models = None
try:
    mlhealth.init(sc=spark.sparkContext, mode=MLOpsMode.PYTHON)
    models = mlhealth.get_model(start_time=0, end_time=2613201446210)
    if models.shape[0] < 2:
        print("Not enough models")
        exit(0)
except Exception:
    print("Models not found")
    exit(1)

mlops.init(spark.sparkContext)

ser = []

for test_mod in models['model'].values:
    if sys.version_info[0] >= 3:
        l2 = pickle.loads(test_mod, encoding='latin1')
    else:
        l2 = pickle.loads(test_mod)

    ser.append(l2)

df = pd.DataFrame(ser) \
       .pct_change() \
       .tail(1)

vector = df.values[0]
headers = df.columns.values

for k,v in zip(headers, vector):
    mlops.set_stat(k, v)
    print("change {} = {}".format(k,v))

spark.stop()
mlops.done()
