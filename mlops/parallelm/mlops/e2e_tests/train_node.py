from datetime import datetime, timedelta
import argparse
import random
import time

from parallelm.mlops import mlops as pm
from parallelm.mlops import StatCategory as st
from parallelm.mlops.stats.multi_line_graph import MultiLineGraph
from parallelm.mlops.stats.table import Table
from parallelm.mlops.stats.bar_graph import BarGraph
from parallelm.mlops.stats.kpi_value import KpiValue
from parallelm.mlops.stats.graph import Graph
from parallelm.mlops.e2e_tests.e2e_constants import E2EConstants


def train_node(options):
    """
    Run the train part of the mlops-tests ion - regardless of the engine used
    :param options: Options provided to node
    :return:
    """

    print("Training test")

    pm.set_stat("Int param", random.randint(0, 20), st.TIME_SERIES)
    pm.set_stat("Double param", random.uniform(0, 20), st.TIME_SERIES)

    # Adding multiple points (to see a graph in the ui), expecting each run to generate 8 points
    pm.set_stat("stat1", 1.0)
    pm.set_stat("stat1", 2.0)
    pm.set_stat("stat1", 3.0)
    pm.set_stat("stat1", 4.0)
    pm.set_stat("stat1", 5.0)
    pm.set_stat("stat1", 6.0)
    pm.set_stat("stat1", 7.0)
    pm.set_stat("stat1", 8.0)

    # Multi line graphs
    mlt = MultiLineGraph().name("Multi Line").labels(["l1", "l2", "l3"]).data([random.randint(0, 10),
                                                                               random.randint(8, 18),
                                                                               random.randint(16, 26)])
    pm.set_stat(mlt)

    # Table
    tbl = Table().name("MyTable").cols(["Date", "Some number"])
    tbl.add_row("1", ["2001Q1", "55"])
    tbl.add_row("2", ["2001Q2", "66"])
    tbl.add_row("3", ["2003Q3", "33"])
    tbl.add_row("4", ["2003Q2", "22"])
    pm.set_stat(tbl)

    # Table
    tbl2 = Table().name("TableNoLabels").cols(["Date", "Some string"])
    tbl2.add_row(["2018Q1", "11"])
    tbl2.add_row(["2018Q2", "22"])
    tbl2.add_row(["2018Q3", "33"])
    tbl2.add_row(["2018Q4", "44"])
    pm.set_stat(tbl2)

    # Bar Graphs
    bar = BarGraph().name("MyBar").cols(["aa", "bb", "cc", "dd", "ee"]).data([10, 15, 12, 9, 8])
    pm.set_stat(bar)

    # General Graph with annotations
    graph = Graph().name("graph").x_title("My X title").y_title("My y title")

    x = list(range(0, 30))
    y1 = list(map(lambda y: y + 10, x))
    y2 = list(map(lambda y: y + 30, x))
    y3 = list(map(lambda y: y + 40, x))

    graph.set_x_series(x)
    graph.add_y_series(label="y1", data=y1)
    graph.add_y_series(label="y2", data=y2)
    graph.add_y_series(label="y3", data=y3)

    graph.annotate(label="ano ano x", x=15)

    pm.set_stat(graph)

    t = time.time()
    val = 3.56
    for i in range(10):
        pm.set_kpi("user-kpi-1", val, t, KpiValue.TIME_SEC)
        t += (1 if i < 5 else -0.7)
        val += 1

    pm.data_alert("mlops-data-alert", "Data alert (anomaly) reported from mlops PySpark", [1, 2, 3])
    pm.health_alert("mlops-mlhealth-alert", "ML-Health (alert) reported from mlops PySpark", [1, 2, 3])
    pm.system_alert("mlops-system-alert", "System alert reported from mlops PySpark", [1, 2, 3])

    print("Done reporting statistics")

    # Generating input statistics

    # TODO: add here test for the histogram generation out of all supported data types (rdd, dataframe)

    # Save model
    print("Output model: {}".format(options.output_model))
    f = open(options.output_model, "w")
    f.write(E2EConstants.MODEL_CONTENT)
    f.close()

    print("Done saving model to {}".format(options.output_model))
