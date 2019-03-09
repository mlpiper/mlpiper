import pandas as pd

from parallelm.mlops import mlops as pm
from parallelm.mlops.mlops_mode import MLOpsMode


# Test is for testing da and health creation for data set containing nan, null, empty strings. Test specifically is for pysaprk g
def test_data_distribution_stat_api_spark(spark_session, generate_da_with_missing_data):
    sc = spark_session.sparkContext
    pm.init(ctx=sc, mlops_mode=MLOpsMode.STAND_ALONE)
    pm._set_api_test_mode()

    pdf = pd.read_csv(generate_da_with_missing_data)
    spark_df = spark_session.createDataFrame(pdf)

    pm.set_data_distribution_stat(data=spark_df)

    sc.stop()

    pm.done()
