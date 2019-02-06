from __future__ import print_function

import time

from datetime import datetime, timedelta
import pandas as pd

from parallelm.mlops import mlops as mlops
from parallelm.mlops.models.model import ModelFormat
from parallelm.mlops.stats.kpi_value import KpiValue
from parallelm.mlops.stats.multi_line_graph import MultiLineGraph
from parallelm.mlops.stats.opaque import Opaque
from parallelm.mlops.e2e_tests.e2e_constants import E2EConstants


def _get_my_agents():
    """
    Return agent list of current ION node
    :return: Agent list used by current ION node
    """

    # Getting the first agent of ion component "0"
    agent_list = mlops.get_agents(mlops.get_current_node().name)
    if len(agent_list) == 0:
        print("Error - must have agents this ion component is running on")
        raise Exception("Agent list is empty")
    return agent_list


def test_stats_basic():
    now = datetime.utcnow()
    last_hour = (now - timedelta(hours=1))

    df = mlops.get_stats("non-existing-stat__AAA", mlapp_node=None, agent=None,
                         start_time=last_hour, end_time=now)

    assert len(df) == 0

    # Adding multiple points (to see a graph in the ui), expecting each run to generate 8 points
    mlops.set_stat("stat1", 1.0)
    mlops.set_stat("stat1", 3.0)
    mlops.set_stat("stat1", 4.0)
    mlops.set_stat("stat1", 5.0)
    mlops.set_stat("stat1", 6.0)
    mlops.set_stat("stat1", 2.0)
    mlops.set_stat("stat1", 7.0)
    mlops.set_stat("stat1", 8.0)

    print("Done reporting statistics")
    time.sleep(10)

    print("MyION = {}".format(mlops.get_mlapp_id()))

    print("Hour before: {}".format(last_hour))
    print("Now:         {}".format(now))

    agent_list = _get_my_agents()

    now = datetime.utcnow()
    last_hour = (now - timedelta(hours=1))
    last_day = (now - timedelta(hours=24))

    # A test for a big time window
    df = mlops.get_stats(name="stat1", mlapp_node=mlops.get_current_node().name,
                         agent=agent_list[0].id, start_time=last_day, end_time=now)

    assert len(df) >= 8, "Got: {} lines in df, expecting at least: {}".format(len(df), 8)

    df = mlops.get_stats(name="stat1", mlapp_node=mlops.get_current_node().name,
                         agent=agent_list[0].id, start_time=last_hour, end_time=now)

    assert len(df) >= 8, "Got: {} lines in df, expecting at least: {}".format(len(df), 8)
    print("Got stat1 statistic\n", df)

    # Another check with the agent object
    df = mlops.get_stats(name="stat1", mlapp_node=mlops.get_current_node().name,
                         agent=agent_list[0], start_time=last_hour, end_time=now)
    print("Got stat1 statistic_2\n", df)

    # Another with node equal to none and agent equal to none
    df = mlops.get_stats(name="stat1", mlapp_node=None, agent=None, start_time=last_hour, end_time=now)
    print("Got stat1 statistic_3\n", df)
    nodes_in_stats = df["node"].tolist()
    if len(nodes_in_stats) > 8:
        print("case_with_stats_from_2_nodes")
        set_nodes = set(nodes_in_stats)
        assert len(set_nodes) > 1


def test_multiline():
    print("Testing multiline")

    data = [[5, 15, 20], [55, 155, 255], [75, 175, 275]]
    columns = ["a", "b", "c"]
    expected_df = pd.DataFrame(data, columns=columns)
    stat_name = "stat-multi-line-test"
    # Multi line graphs
    for row in data:
        mlt = MultiLineGraph().name(stat_name).labels(columns).data(row)
        mlops.set_stat(mlt)

    time.sleep(10)
    agent_list = _get_my_agents()
    now = datetime.utcnow()
    last_hour = (now - timedelta(hours=1))

    df = mlops.get_stats(name=stat_name, mlapp_node=mlops.get_current_node().name,
                         agent=agent_list[0].id, start_time=last_hour, end_time=now)

    print("Got multiline df\n", df)
    df_tail = pd.DataFrame(df.tail(len(data))).reset_index(drop=True)
    print("df_tail:\n", df_tail)
    df_only_cols = df_tail[columns]
    print("Tail of multiline df + only relevant cols\n", df_only_cols)
    print("Expected:\n", expected_df)

    # We expect the tail (last rows) of the result df to be like the expected df.
    assert expected_df.equals(
        df_only_cols), "Expected multiline is not equal to obtained\n-------\n{}\n-------\n{}\n".format(
        expected_df, df_only_cols)


def _get_curr_test_cycle():
    now = datetime.utcnow()
    last_4hour = (now - timedelta(hours=4))

    df = mlops.get_stats(name=E2EConstants.E2E_RUN_STAT, mlapp_node=None, agent=None, start_time=last_4hour,
                         end_time=now)
    return len(df)


def test_kpi_basic():
    test_cycle = _get_curr_test_cycle()
    print("KPI-TEST: {}".format(test_cycle))
    sec_now = int(time.time())
    sec_4h_ago = sec_now - (3600 * 4)
    kpi_window_start = sec_4h_ago
    val = 3.56
    nr_kpi_point = 10
    kpi_name = "test-kpi-1"
    kpi_window_end = kpi_window_start
    for i in range(nr_kpi_point):
        mlops.set_kpi(kpi_name, val, kpi_window_end, KpiValue.TIME_SEC)
        kpi_window_end += 1
        val += 1

    time.sleep(10)

    kpi_datetime_start = datetime.utcfromtimestamp(kpi_window_start)
    kpi_datetime_end = datetime.utcfromtimestamp(kpi_window_end)

    print("datetime start: {}".format(kpi_datetime_start))
    print("datetime end:   {}".format(kpi_datetime_end))

    df = mlops.get_kpi(name=kpi_name, start_time=kpi_datetime_start, end_time=kpi_datetime_end)
    print(df)
    if len(df) != nr_kpi_point:
        raise Exception("Got: {} kpi points, expecting: {}".format(len(df), nr_kpi_point))


def test_opaque():
    test_cycle = _get_curr_test_cycle()
    print("OPAQUE-TEST: {}".format(test_cycle))

    obj = {"1": "aaaaaa", "2": 33}

    stat_name = "opq-1"
    opq = Opaque().name(stat_name).data(obj)
    mlops.set_stat(opq)

    print("Done reporting opaque statistics")
    time.sleep(10)

    now = datetime.utcnow()
    last_day = (now - timedelta(hours=24))

    df = mlops.get_stats(name=stat_name, start_time=last_day, end_time=datetime.utcnow(), mlapp_node=None, agent=None)

    assert len(df) >= (test_cycle + 1), "Got: {} lines in df, expecting at least: {}".format(len(df), 1)

    for idx in range(0, test_cycle + 1):
        data = df.iloc[idx]["data"]
        assert data["1"] == obj["1"]
        assert data["2"] == obj["2"]


def test_stats_model():
    # Publishing stats with model

    model = mlops.Model(model_format=ModelFormat.SPARKML)
    # Adding multiple points (to see a graph in the ui), expecting each run to generate 8 points
    model.set_stat("model_stat1", 1.0)
    model.set_stat("model_stat1", 3.0)
    model.set_stat("model_stat1", 4.0)
    model.set_stat("model_stat1", 5.0)
    model.set_stat("model_stat1", 6.0)
    model.set_stat("model_stat1", 2.0)
    model.set_stat("model_stat1", 7.0)
    model.set_stat("model_stat1", 8.0)

    # Multi line graphs
    stat_name = "model_stat-multi-line-test"
    data = [[5, 15, 20], [55, 155, 255], [75, 175, 275]]
    columns = ["a", "b", "c"]

    for row in data:
        mlt = MultiLineGraph().name(stat_name).labels(columns).data(row)
        model.set_stat(mlt)

    # KPI stats
    sec_now = int(time.time())
    kpi_window_start = sec_now
    val = 3.56
    nr_kpi_point = 10
    kpi_name = "model_test-kpi-1"
    kpi_window_end = kpi_window_start
    for i in range(nr_kpi_point):
        model.set_kpi(kpi_name, val, kpi_window_end, KpiValue.TIME_SEC)
        kpi_window_end += 1
        val += 1

    # opaque stats
    obj = {"1": "aaaaaa", "2": 33}

    stat_name = "opq-1"
    opq = Opaque().name(stat_name).data(obj)
    model.set_stat(opq)

    # TODO fetch all stats per model id
