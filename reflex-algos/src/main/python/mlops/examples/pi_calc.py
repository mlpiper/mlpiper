from __future__ import print_function

import sys
from random import random
from operator import add

from pyspark.sql import SparkSession

from parallelm.mlops import mlops
from parallelm.mlops.stats.multi_line_graph import MultiLineGraph
from parallelm.mlops.stats.table import Table
from parallelm.mlops.stats.bar_graph import BarGraph


if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonPi")\
        .getOrCreate()

    sc = spark.sparkContext

    mlops.init(spark.sparkContext)

    # Basic numbers (points in line graphs)
    mlops.set_stat("myCounterDouble", 5.5)
    mlops.set_stat("myCounterDouble2", 7.3)

    # Multi-line graph
    mlt = MultiLineGraph().name("Multi Line").labels(["l1", "l2"]).data([5, 16])
    mlops.set_stat(mlt)

    # Example of sending a table to pm system.
    # Multi-line graphs
    mlt = MultiLineGraph().name("Multi Line").labels(["l1", "l2"]).data([5, 16])
    mlops.set_stat(mlt)

    # Table example
    tbl = Table().name("MyTable").cols(["", "Date"])
    tbl.add_row(["line 1", "2001Q1"])
    tbl.add_row(["line 2", "2014Q3"])
    mlops.set_stat(tbl)

    bar = BarGraph().name("MyBar").cols(["aa", "bb", "cc", "dd", "ee"]).data([10, 15, 12, 9, 8])
    mlops.set_stat(bar)

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions

    def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    print("Pi is roughly %f" % (4.0 * count / n))

    spark.stop()
    mlops.done()

