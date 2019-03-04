import pytest
import requests_mock


from parallelm.mlops import mlops as pm
from ion_test_helper import set_mlops_env, ION1, test_models_info, test_health_info
from ion_test_helper import test_workflow_instances, test_ee_info, test_agents_info
from parallelm.mlops.mlops_rest_factory import MlOpsRestFactory
from parallelm.mlops.mlops_mode import MLOpsMode

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


def test_attach():

    mlapp_id = "144a045d-c927-4afb-a85c-5224bd68f1bb"

    ion_instance_id = ION1.ION_INSTANCE_ID
    ion_node_id = ION1.NODE_1_ID
    token = ION1.TOKEN

    set_mlops_env(ion_id=ion_instance_id, ion_node_id=ion_node_id, token=token, model_id=ION1.MODEL_ID)
    rest_helper = MlOpsRestFactory().get_rest_helper(MLOpsMode.AGENT, mlops_server="localhost",
                                                     mlops_port="3456", token=token)

    with requests_mock.mock() as m:
        m.get(rest_helper.url_get_workflow_instance(ion_instance_id), json=test_workflow_instances)
        m.get(rest_helper.url_get_ees(), json=test_ee_info)
        m.get(rest_helper.url_get_agents(), json=test_agents_info)
        m.get(rest_helper.url_get_model_list(), json=test_models_info)
        m.get(rest_helper.url_get_health_thresholds(ion_instance_id), json=test_health_info)
        m.get(rest_helper.url_get_model_stats(ION1.MODEL_ID), json=test_model_stats)
        m.get(rest_helper.url_get_uuid("model"), json={"id": "model_5906255e-0a3d-4fef-8653-8d41911264fb"})
        m.post(rest_helper.url_login(), json={"token": token})

        pm.attach(mlapp_id=ION1.ION_INSTANCE_ID, mlops_server="localhost", mlops_port=3456, password="nimda1")
        mlapp_id_ret = pm.get_mlapp_id()
        assert (mlapp_id_ret == ION1.ION_ID)

        mlapp_policy_ret = pm.get_mlapp_policy()
        assert (str(mlapp_policy_ret) == "Policy:\nhealthThreshold: 0.2\ncanaryThreshold: 0.5\n")
        pm.done()
