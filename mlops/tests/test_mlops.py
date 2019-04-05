import os
import time
import uuid
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest
import requests_mock

from ion_test_helper import set_mlops_env, ION1, test_models_info, test_health_info
from ion_test_helper import test_workflow_instances, test_ee_info, test_agents_info
from parallelm.mlops import Versions
from parallelm.mlops import mlops as pm
from parallelm.mlops.constants import Constants
from parallelm.mlops.events.event_type import EventType
from parallelm.mlops.ion.ion import Agent
from parallelm.mlops.mlops_exception import MLOpsException, MLOpsConnectionException
from parallelm.mlops.mlops_mode import MLOpsMode
from parallelm.mlops.mlops_rest_factory import MlOpsRestFactory
from parallelm.mlops.models.model import ModelFormat
from parallelm.mlops.stats.bar_graph import BarGraph
from parallelm.mlops.stats.graph import Graph, MultiGraph
from parallelm.mlops.stats.kpi_value import KpiValue
from parallelm.mlops.stats.multi_line_graph import MultiLineGraph
from parallelm.mlops.stats.table import Table
from parallelm.mlops.stats_category import StatCategory
from parallelm.mlops.versions_info import mlops_version_info

test_model_stats = [{
    "data": "{\" bitrate\":["
            "{\"-inf to -1705923.8134\":0.0},"
            "{\"-1705923.8134 to -1207382.1370700002\":0.0},"
            "{\"-1207382.1370700002 to -708840.4607400001\":0.0},"
            "{\"-708840.4607400001 to -210298.78441000008\":0.0},"
            "{\"-210298.78441000008 to 288242.89191999985\":0.4138171667829728},"
            "{\"288242.89191999985 to 786784.5682499998\":0.3480460572226099},"
            "{\"786784.5682499998 to 1285326.24458\":0.0915910676901605},"
            "{\"1285326.24458 to 1783867.9209099996\":0.0183182135380321},"
            "{\"1783867.9209099996 to 2282409.59724\":0.0183182135380321},"
            "{\"2282409.59724 to 2780951.2735699993\":0.0366364270760642},"
            "{\"2780951.2735699993 to 3279492.9498999994\":0.0366364270760642},"
            "{\"3279492.9498999994 to +inf\":0.0366364270760642}],"
            "\"graphType\":\"BARGRAPH\","
            "\"timestamp\":1533242895931000064,"
            "\"mode\":\"INSTANT\","
            "\"name\":\"continuousDataHistogram\","
            "\"type\":\"Health\"}",
    "type": "MLHealthModel",
    "id": "f26aeec0-18c2-455d-aaf5-136e17cc829d"
}]


def test_mlops_versioning():
    # 0.9.0 is not compatible with 1.0.0 and >
    with pytest.raises(MLOpsException):
        pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE, version=Versions.VERSION_0_0_9)

    # Testing that if the current version is not registered (by mistake) mlops.init will fail
    with pytest.raises(MLOpsException):
        vv = mlops_version_info.get_curr_version()
        mlops_version_info.set_curr_version('x.x.x')
        pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)
    mlops_version_info.set_curr_version(vv)

    # This to check that compatible versions are ok.
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE, version=Versions.VERSION_1_0_0)
    pm.done()

    # This to check that we are compatible with the same version
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE, version=mlops_version_info.get_curr_version())
    pm.done()


def test_mlops_structure_api():
    ion_instance_id = ION1.ION_INSTANCE_ID
    ion_node_id = ION1.NODE_1_ID
    token = ION1.TOKEN

    set_mlops_env(ion_id=ion_instance_id, ion_node_id=ion_node_id, token=token, model_id=ION1.MODEL_ID)
    rest_helper = MlOpsRestFactory().get_rest_helper(MLOpsMode.AGENT, mlops_server="localhost",
                                                     mlops_port="3456", token=token)

    rest_helper.set_prefix(Constants.URL_MLOPS_PREFIX)
    with requests_mock.mock() as m:
        m.get(rest_helper.url_get_workflow_instance(ion_instance_id), json=test_workflow_instances)
        m.get(rest_helper.url_get_ees(), json=test_ee_info)
        m.get(rest_helper.url_get_agents(), json=test_agents_info)
        m.get(rest_helper.url_get_model_list(), json=test_models_info)
        m.get(rest_helper.url_get_health_thresholds(ion_instance_id), json=test_health_info)
        m.get(rest_helper.url_get_model_stats(ION1.MODEL_ID), json=test_model_stats)
        m.get(rest_helper.url_get_uuid("model"), json={"id": "model_5906255e-0a3d-4fef-8653-8d41911264fb"})

        pm.init(ctx=None, mlops_mode=MLOpsMode.AGENT)
        assert pm.get_mlapp_id() == ION1.ION_ID
        assert pm.get_mlapp_name() == ION1.ION_NAME

        curr_node = pm.get_current_node()
        assert curr_node.id == ion_node_id

        nodes = pm.get_nodes()
        assert len(nodes) == 2

        node0 = pm.get_node('1')
        assert node0 is not None
        assert node0.pipeline_pattern_id == ION1.PIPELINE_PATTERN_ID_1
        assert node0.pipeline_instance_id == ION1.PIPELINE_INST_ID_1

        node0_agents = pm.get_agents('1')
        assert len(node0_agents) == 1
        assert node0_agents[0].id == ION1.AGENT_ID_0
        assert node0_agents[0].hostname == 'localhost'

        agent = pm.get_agent('1', ION1.AGENT_ID_0)
        assert agent.id == ION1.AGENT_ID_0
        assert agent.hostname == 'localhost'

        model = pm.current_model()
        assert model is not None
        assert model.metadata.modelId == ION1.MODEL_ID

        pm.done()


def test_suppress_connection_errors():
    import requests
    from parallelm.mlops.events.event import Event
    from parallelm.mlops.mlops_env_constants import MLOpsEnvConstants

    ion_instance_id = ION1.ION_INSTANCE_ID
    ion_node_id = ION1.NODE_1_ID
    token = ION1.TOKEN
    pipeline_instance_id = ION1.PIPELINE_INST_ID_1

    set_mlops_env(ion_id=ion_instance_id, ion_node_id=ion_node_id, token=token, model_id=ION1.MODEL_ID)

    os.environ[MLOpsEnvConstants.MLOPS_AGENT_PUBLIC_ADDRESS] = "placeholder"
    rest_helper = MlOpsRestFactory().get_rest_helper(MLOpsMode.AGENT, mlops_server="localhost",
                                                     mlops_port="3456", token=token)

    rest_helper.set_prefix(Constants.URL_MLOPS_PREFIX)

    with requests_mock.mock() as m:
        m.get(rest_helper.url_get_workflow_instance(ion_instance_id), json=test_workflow_instances)
        m.get(rest_helper.url_get_health_thresholds(ion_instance_id), json=test_health_info)
        m.get(rest_helper.url_get_ees(), json=test_ee_info)
        m.get(rest_helper.url_get_agents(), json=test_agents_info)
        m.get(rest_helper.url_get_model_list(), json=test_models_info)
        m.get(rest_helper.url_get_model_stats(ION1.MODEL_ID), json=test_model_stats)

        m.post(rest_helper.url_post_event(pipeline_instance_id), exc=requests.exceptions.ConnectionError)

        pm.init(ctx=None, mlops_mode=MLOpsMode.AGENT)

        event_obj = Event(label="event_name", event_type=EventType.System, description=None, data="123",
                          is_alert=False, timestamp=None)

        with pytest.raises(MLOpsConnectionException):
            pm.set_event(name="event_name", data="123", type=EventType.System)

        with pytest.raises(MLOpsConnectionException):
            pm.event(event_obj)

        pm.suppress_connection_errors(True)
        pm.set_event(name="event_name", data="123", type=EventType.System)
        pm.event(event_obj)
        pm.suppress_connection_errors(False)

        pm.done()


def test_init_done():
    """
    Testing api for information such as ION id, ion name and such
    :return:
    """
    with pytest.raises(MLOpsException):
        pm.set_stat("st1", 5.5)

    with pytest.raises(MLOpsException):
        pm.done()

    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)
    pm.done()


def test_set_stat_basic():
    with pytest.raises(MLOpsException):
        pm.set_stat(name=None, data=None)

    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)
    pm.set_stat("st1", data=5.5, category=StatCategory.TIME_SERIES)
    pm.set_stat("st1", data=5.5)

    pm.done()


def test_table():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    with pytest.raises(MLOpsException):
        Table().name("mytable").cols(["a", "b", "c"]).add_row([1, 2, 3]).add_row([1, 2])

    with pytest.raises(MLOpsException):
        tbl = Table().name("mytable").cols(["a", "b"])
        pm.set_stat(tbl)

    tbl = Table().name("good-1").cols(["a", "b", "c"]).add_rows([[1, 2, 3], [1, 2, 3]])
    pm.set_stat(tbl)

    tbl = Table().name("good-2").cols(["a", "b", "c"])
    tbl.add_row("r1", [1, 2, 3])
    tbl.add_row("r2", [3, 4, 5])
    pm.set_stat(tbl)

    tbl = Table().name("good-3").cols(["a", "b", "c"])
    tbl.add_row([6, 7, 8])
    tbl.add_row([9, 0, 1])
    pm.set_stat(tbl)

    pm.done()


def test_multiline_graph():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    with pytest.raises(MLOpsException):
        MultiLineGraph().name("mlt").labels([1, 2, 3])

    with pytest.raises(MLOpsException):
        MultiLineGraph().name("mlt").labels(["g1", "g2"]).data(["aa", "bb"])

    with pytest.raises(MLOpsException):
        MultiLineGraph().name("mlt").data(["aa", "bb"])

    with pytest.raises(MLOpsException):
        mlt = MultiLineGraph().name("mlt").labels(["g1"]).data([55, 66])
        pm.set_stat(mlt)

    mlt = MultiLineGraph().name("mlt").labels(["g1", "g2"]).data([55, 66])
    pm.set_stat(mlt)
    pm.done()


def test_bar_graph():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    with pytest.raises(MLOpsException):
        BarGraph().name("bar").cols(["g1", "g2"]).data(["aa", "bb"])

    with pytest.raises(MLOpsException):
        BarGraph().name("bar").data(["aa", "bb"])

    with pytest.raises(MLOpsException):
        mlt = BarGraph().name("mlt").cols(["g1"]).data([55, 66])
        pm.set_stat(mlt)

    with pytest.raises(MLOpsException):
        mlt_cont = BarGraph().name("mlt").cols([1, 2]).data([55, 66]).as_continuous()
        pm.set_stat(mlt_cont)

    mlt = BarGraph().name("mlt").cols(["g1", "g2"]).data([55, 66])
    pm.set_stat(mlt)

    mlt_cont = BarGraph().name("mlt").cols([1, 2, 3]).data([55, 66]).as_continuous()
    pm.set_stat(mlt_cont)

    pm.done()


def test_general_graph():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)
    with pytest.raises(MLOpsException):
        Graph().name("gg").set_x_series("not-a-vec")

    with pytest.raises(MLOpsException):
        gg = Graph().name("gg").set_x_series([1, 2, 3, 4, 5, 6])
        gg.get_mlops_stat(None)

    with pytest.raises(MLOpsException):
        # Y is not the same size of X
        Graph().name("gg").set_x_series([1, 2, 3, 4, 5, 6]).add_y_series(label="rate", data=[1, 2, 3, 4, 5])

    x_series = [1, 2, 3, 4, 5, 6]
    y1 = [11, 12, 13, 14, 15, 16]

    gg = Graph().name("gg").set_x_series(x_series).add_y_series(label="y1", data=y1)
    pm.set_stat(gg)
    pm.done()


def test_multi_graph():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)
    with pytest.raises(MLOpsException):
        MultiGraph().name("mg").add_series(label="a", x="not-a-vec", y="not-a-vec")

    with pytest.raises(MLOpsException):
        MultiGraph().name("gg").add_series(label="a", x=[1, 2], y=["a", "b"])

    with pytest.raises(MLOpsException):
        # Y is not the same size of X
        MultiGraph().name("gg").add_series(x=[1, 2, 3, 4, 5, 6], label="rate", y=[1, 2, 3, 4, 5])

    x1_series = [0, 2, 4, 6]
    y1_series = [11, 12, 13, 14]

    x2_series = [1, 3, 5, 7]
    y2_series = [15, 16, 17, 18]

    gg = MultiGraph().name("gg")
    gg.add_series(x=x1_series, label="y1", y=y1_series)
    gg.add_series(x=x2_series, label="y2", y=y2_series)
    pm.set_stat(gg)
    pm.done()


def test_get_stats_api():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)
    pm._set_api_test_mode()

    now = datetime.utcnow()
    last_hour = (now - timedelta(hours=1))

    with pytest.raises(MLOpsException):
        pm.get_stats(name=None, mlapp_node="33", agent=1, start_time=None, end_time=None)

    with pytest.raises(MLOpsException):
        pm.get_stats(name="stat_1", mlapp_node="33", agent="aaa", start_time=None, end_time=None)

    with pytest.raises(MLOpsException):
        pm.get_stats(name="stat_1", mlapp_node="33", agent="aaa", start_time=last_hour, end_time=None)

    with pytest.raises(MLOpsException):
        pm.get_stats(name="stat_1", mlapp_node="33", agent="aaa", start_time=None, end_time=now)

    with pytest.raises(MLOpsException):
        pm.get_stats(name="stat_1", mlapp_node="33", agent=1, start_time=last_hour, end_time=now)

    with pytest.raises(MLOpsException):
        pm.get_stats(name="stat_1", mlapp_node=None, agent=None, start_time=now, end_time=last_hour)

    agent_obj = Agent()
    pm.get_stats(name="stat_1", mlapp_node="1", agent=agent_obj, start_time=last_hour, end_time=now)

    with pytest.raises(MLOpsException):
        pm.get_stats(name="stat_1", mlapp_node=None, agent=agent_obj, start_time=last_hour, end_time=now)

    pm.get_stats(name="stat_1", mlapp_node=None, agent=None, start_time=last_hour, end_time=now)

    pm.done()


def test_kpi_api():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)
    pm._set_api_test_mode()

    with pytest.raises(MLOpsException):
        ts = time.time()
        pm.set_kpi(None, 5.5, ts, KpiValue.TIME_NSEC)

    with pytest.raises(MLOpsException):
        ts = time.time()
        pm.set_kpi("user-kpi-1", ["1"], ts, KpiValue.TIME_SEC)

    with pytest.raises(MLOpsException):
        ts = time.time()
        pm.set_kpi("user-kpi-1", 5.5, ts, "no-such-time-unit")

    with pytest.raises(MLOpsException):
        pm.get_kpi("user-kpi-1", None, None)

    now = datetime.utcnow()
    last_hour = (now - timedelta(hours=1))

    pm.get_kpi("user-kpi-1", last_hour, now)
    pm.done()


def test_data_distribution_stat_api(generate_da_with_missing_data):
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)
    pm._set_api_test_mode()

    # basic test
    data = np.array([[1, 2], [3, 4]])
    pm.set_data_distribution_stat(data)

    # test with column missing
    blah = pd.read_csv(generate_da_with_missing_data)
    pm.set_data_distribution_stat(blah)
    pm.done()


def test_get_models_api():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)
    pm._set_api_test_mode()

    with pytest.raises(MLOpsException):
        pm.get_models_by_time(start_time=None, end_time=None)

    pm.done()


def test_set_events_api():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)
    pm._set_api_test_mode()

    with pytest.raises(MLOpsException):
        pm.set_event(name=None, data=None, type=EventType.System)

    with pytest.raises(MLOpsException):
        pm.set_event(name="ssss", data=None, type=None)

    pm.done()


def test_publish_model_api():
    pm.init(ctx=None, mlops_mode=MLOpsMode.STAND_ALONE)

    model_data = "MODEL_DATA"
    model = pm.Model(name="my model", model_format=ModelFormat.TEXT, description="test model",
                     user_defined="whatever I want goes here")

    model_file = os.path.join(os.path.sep, "tmp", str(uuid.uuid4()))
    f = open(model_file, 'w')
    f.write(model_data)
    f.close()

    model.set_model_path(model_file)

    model_id = pm.publish_model(model)
    assert (model_id == model.get_id())
    os.remove(model_file)

    model_df = pm.get_model_by_id(model_id, download=True)

    pm.done()

    # accessing 0th row, 'data' column of returned model dataframe
    assert (model_data == model_df.iloc[0]['data'])
