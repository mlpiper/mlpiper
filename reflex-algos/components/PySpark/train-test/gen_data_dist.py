import numpy as np

from pyspark.ml.linalg import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SQLContext, HiveContext, Row, SparkSession
from pyspark.sql.functions import col, count, rand
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, VectorIndexer, StringIndexer
from pyspark.ml import Pipeline
from pyspark.sql.functions import sum, sqrt, min, max

mlops_loaded = False
try:
    from parallelm.mlops import mlops
    from parallelm.mlops.models.model import ModelFormat
    from parallelm.mlops.e2e_tests.e2e_constants import E2EConstants
    mlops_loaded = True
except ImportError:
    pass


def generate_dataset(num_attr, num_rows, K, spark_ctx):
    """
    The function generates a random dataset
    """
    spark_session = SparkSession(spark_ctx)


    npds = np.zeros((num_rows, num_attr))
    centers = np.zeros((K, num_attr))
    for i in range (0,K):
        centers[i,i] = 10
    for element_index in range (0,num_rows):
        npds[element_index,:] = centers[np.random.randint(0,K),:] + np.random.randn(num_attr)
    data_set = spark_ctx.parallelize(npds).map(lambda x: x.tolist()).toDF()
    return data_set


def gen_data_dist_stats(spark_ctx):

    spark_session = SparkSession(spark_ctx)

    # Import Data
    ##################################
    K = 3  # fixed number of centers
    num_attr = 10  # fixed number of attributes
    num_rows = 60000  # number of rows in the dataset
    input_data = generate_dataset(num_attr, num_rows, K, spark_ctx)

    column_names_all = input_data.columns
    for col_index in range(0, len(column_names_all)):
        input_data = input_data.withColumnRenamed(column_names_all[col_index], 'c' + str(col_index))

    input_data = input_data.cache()

    input_train = input_data

    # SparkML pipeline
    ##################################
    exclude_cols = []
    column_names = input_train.columns
    input_col_names = []
    for elmts in column_names:
        ind = True
        for excludes in exclude_cols:
            if elmts == excludes:
                ind = False
        if ind:
            input_col_names.append(elmts)
    print(input_col_names)

    vector_assembler = VectorAssembler(
        inputCols=input_col_names,
        outputCol="features")

    kmeans_pipe = KMeans(
        k=K,
        initMode="k-means||",
        initSteps=5,
        tol=1e-4,
        maxIter=100,
        featuresCol="features")
    full_pipe = [vector_assembler, kmeans_pipe]
    model_kmeans = Pipeline(stages=full_pipe).fit(input_train)

    if mlops_loaded:
        try:
            mlops.set_data_distribution_stat(data=input_train, model=model_kmeans)
            m = mlops.Model(model_format=ModelFormat.SPARKML)
            m.set_data_distribution_stat(data=input_train)
            print("PM: done generating histogram")
        except Exception as e:
            print("PM: failed to generate histogram using pm.stat")
            print(e)

        # Indicating that model statistics were reported
        mlops.set_stat(E2EConstants.MODEL_STATS_REPORTED_STAT_NAME, 1)

    return model_kmeans


