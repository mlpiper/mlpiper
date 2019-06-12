import pytest

import requests_mock
import json
import os
import uuid
from datetime import datetime

from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.mlops_mode import MLOpsMode
from parallelm.mlops.models.model import ModelFormat
from parallelm.mlops.models.model_helper import ModelHelper
from parallelm.mlops.models.model_filter import ModelFilter
from parallelm.mlops import models
from parallelm.mlops.ion.ion import ION
from parallelm.mlops.mlops_rest_factory import MlOpsRestFactory
from parallelm.mlops import mlops
from parallelm.mlops.constants import Constants
from ion_test_helper import set_mlops_env, ION1, test_models_info, test_health_info
from ion_test_helper import test_workflow_instances, test_ee_info, test_agents_info

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

models_list_json_dict = [
    {
        models.json_fields.MODEL_ID_FIELD: u'8c95deaf-87e4-4c21-bc92-e5b1a0454f9a',
        models.json_fields.MODEL_FORMAT_FIELD: 'TEXT',
        models.json_fields.MODEL_SIZE_FIELD: 0,
        models.json_fields.MODEL_NAME_FIELD: 'model-4',
        models.json_fields.MODEL_CREATED_ON_FIELD: 1518460283900
    },
    {
        models.json_fields.MODEL_ID_FIELD: u'9d1d4a81-29a0-492f-a6c7-d35489250368',
        models.json_fields.MODEL_FORMAT_FIELD: u'PMML',
        models.json_fields.MODEL_SIZE_FIELD: 0,
        models.json_fields.MODEL_NAME_FIELD: u'model-8',
        models.json_fields.MODEL_CREATED_ON_FIELD: 1518460573573
    },
]


def test_create_model():
    with requests_mock.mock() as m:
        rh = MlOpsRestFactory().get_rest_helper(MLOpsMode.AGENT)

        model_id = "model_5906255e-0a3d-4fef-8653-8d41911264fb"
        m.get(rh.url_get_uuid("model"), json={"id": model_id})

        ion = ION()
        ion.id = "bdc2ee10-767c-4524-ba72-8268a3894bff"
        mh = ModelHelper(rest_helper=rh, ion=ion, stats_helper=None)

        model_data = "MODEL_DATA"
        model = mh.create_model(name="my model", model_format=ModelFormat.TEXT, description="test model")

        model_file = os.path.join(os.path.sep, "tmp", str(uuid.uuid4()))
        f = open(model_file, 'w')
        f.write(model_data)
        f.close()

        model.set_model_path(model_file)

        assert model.get_id() == model_id
        os.remove(model_file)

        rh.done()


def test_model_list_dict_from_json():
    with requests_mock.mock() as m:
        m.get('http://localhost:3456/v1/models', json=models_list_json_dict)

        rh = MlOpsRestFactory().get_rest_helper(MLOpsMode.AGENT)
        ion = ION()
        ion.id = "bdc2ee10-767c-4524-ba72-8268a3894bff"
        mh = ModelHelper(rest_helper=rh, ion=ion, stats_helper=None)

        result_model_list = mh.fetch_all_models_json_dict()
        print("Type is: {}".format(type(result_model_list)))
        print("result_model_list: {}".format(result_model_list))
        json_str_orig = json.dumps(models_list_json_dict, sort_keys=True, indent=2)
        json_str_got = json.dumps(result_model_list, sort_keys=True, indent=2)
        assert json_str_orig == json_str_got
        rh.done()


def test_convert_models_json_dict_to_dataframe():
    rh = MlOpsRestFactory().get_rest_helper(MLOpsMode.AGENT)
    ion = ION()
    ion.id = "bdc2ee10-767c-4524-ba72-8268a3894bff"
    mh = ModelHelper(rest_helper=rh, ion=ion, stats_helper=None)

    df = mh.convert_models_json_dict_to_dataframe(models_list_json_dict)
    assert len(df) == 2
    rh.done()


def test_get_models_with_filter():
    with requests_mock.mock() as m:
        m.get('http://localhost:3456/v1/models', json=models_list_json_dict)

        rh = MlOpsRestFactory().get_rest_helper(MLOpsMode.AGENT)
        ion = ION()
        ion.id = "bdc2ee10-767c-4524-ba72-8268a3894bff"
        mh = ModelHelper(rest_helper=rh, ion=ion, stats_helper=None)

        mf = ModelFilter()
        mf.time_window_start = datetime.utcfromtimestamp(1518460571573 / 1000)
        mf.time_window_end = datetime.utcfromtimestamp(1518460577573 / 1000)

        filtered_models = mh.get_models_dataframe(model_filter=mf, download=False)
        assert len(filtered_models) == 1
        print(filtered_models[[models.json_fields.MODEL_NAME_FIELD, models.json_fields.MODEL_CREATED_ON_FIELD]])
        rh.done()


def test_get_models_with_filter_2():
    with requests_mock.mock() as m:
        m.get('http://localhost:3456/v1/models', json=models_list_json_dict)

        rh = MlOpsRestFactory().get_rest_helper(MLOpsMode.AGENT)
        ion = ION()
        ion.id = "13445bb4-535a-4d45-b2f2-77293026e3da"
        mh = ModelHelper(rest_helper=rh, ion=ion, stats_helper=None)

        model_id_to_filter = '8c95deaf-87e4-4c21-bc92-e5b1a0454f9a'
        mf = ModelFilter()
        mf.id = model_id_to_filter

        filtered_models = mh.get_models_dataframe(model_filter=mf, download=False)
        print(filtered_models[[models.json_fields.MODEL_ID_FIELD, models.json_fields.MODEL_FORMAT_FIELD]])
        assert len(filtered_models) == 1
        assert filtered_models.iloc[0][models.json_fields.MODEL_FORMAT_FIELD] == 'TEXT'
        assert filtered_models.iloc[0][models.json_fields.MODEL_ID_FIELD] == model_id_to_filter
        rh.done()


def test_get_models_with_filter_3():
    with requests_mock.mock() as m:
        m.get('http://localhost:3456/v1/models', json=models_list_json_dict)

        rh = MlOpsRestFactory().get_rest_helper(MLOpsMode.AGENT)
        ion = ION()
        ion.id = "bdc2ee10-767c-4524-ba72-8268a3894bff"
        mh = ModelHelper(rest_helper=rh, ion=ion, stats_helper=None)

        mf = ModelFilter()
        mf.time_window_start = datetime.utcfromtimestamp(1518460571573 / 1000)
        mf.time_window_end = datetime.utcfromtimestamp(1518460577573 / 1000)

        filtered_models = mh.get_models_dataframe(model_filter=mf, download=False)
        assert len(filtered_models) == 1
        print(filtered_models[[models.json_fields.MODEL_NAME_FIELD, models.json_fields.MODEL_CREATED_ON_FIELD]])
        # No model found
        mf.id = "111111111111111"
        filtered_models = mh.get_models_dataframe(model_filter=mf, download=False)
        assert len(filtered_models) == 0
        rh.done()


def test_publish_model():
    expected_models_list_json_dict = [
        {
            models.json_fields.MODEL_ID_FIELD: '',
            models.json_fields.MODEL_NAME_FIELD: 'my model name',
            models.json_fields.MODEL_FORMAT_FIELD: 'Text',
            models.json_fields.MODEL_VERSION_FIELD: '',
            models.json_fields.MODEL_DESCRIPTION_FIELD: 'test model',
            models.json_fields.MODEL_TRAIN_VERSION_FIELD: '',
            models.json_fields.MODEL_SIZE_FIELD: 10,
            models.json_fields.MODEL_OWNER_FIELD: '',
            models.json_fields.MODEL_CREATED_ON_FIELD: None,
            models.json_fields.MODEL_FLAG_VALUES_FIELD: [],
            models.json_fields.MODEL_ANNOTATIONS_FIELD: {"custom_data": "my content"},
            models.json_fields.MODEL_ACTIVE_FIELD: False
        }
    ]

    rh = MlOpsRestFactory().get_rest_helper(MLOpsMode.STAND_ALONE)
    ion = ION()
    mh = ModelHelper(rest_helper=rh, ion=ion, stats_helper=None)

    model_data = "MODEL_DATA"
    model = mh.create_model(name="my model name", model_format=ModelFormat.TEXT, description="test model")
    model.set_annotations({"custom_data": "my content"})

    model_file = os.path.join(os.path.sep, "tmp", str(uuid.uuid4()))
    f = open(model_file, 'w')
    f.write(model_data)
    f.close()
    model.set_model_path(model_file)

    my_id = mh.publish_model(model, None)
    os.remove(model_file)
    assert my_id == model.get_id()
    expected_models_list_json_dict[0][models.json_fields.MODEL_ID_FIELD] = my_id

    ret_data = mh.download_model(my_id)
    assert ret_data == model_data

    result_model_list = mh.fetch_all_models_json_dict()

    actual_json_dumps = json.dumps(result_model_list, sort_keys=True, indent=2)
    local_json_dump = json.dumps(expected_models_list_json_dict, sort_keys=True, indent=2)
    print("Expected_Dumps: {}".format(local_json_dump))
    print("Actual_Dumps: {}".format(actual_json_dumps))

    assert expected_models_list_json_dict == result_model_list

    with pytest.raises(MLOpsException):
        mh.publish_model("Not a model", None)
    rh.done()


def test_feature_importance():
    num_significant_features = 6
    ion_instance_id = ION1.ION_INSTANCE_ID
    ion_node_id = ION1.NODE_1_ID
    pipeline_instance_id = ION1.PIPELINE_INST_ID_1
    set_mlops_env(ion_id=ion_instance_id, ion_node_id=ion_node_id, model_id=ION1.MODEL_ID)
    rest_helper = MlOpsRestFactory().get_rest_helper(MLOpsMode.AGENT, mlops_server="localhost",
                                                     mlops_port="3456", token="")
    rest_helper.set_prefix(Constants.URL_MLOPS_PREFIX)
    with requests_mock.mock() as m:
        m.get(rest_helper.url_get_workflow_instance(ion_instance_id), json=test_workflow_instances)
        m.get(rest_helper.url_get_ees(), json=test_ee_info)
        m.get(rest_helper.url_get_agents(), json=test_agents_info)
        m.get(rest_helper.url_get_model_list(), json=test_models_info)
        m.get(rest_helper.url_get_health_thresholds(ion_instance_id), json=test_health_info)
        m.get(rest_helper.url_get_model_stats(ION1.MODEL_ID), json=test_model_stats)
        m.get(rest_helper.url_get_uuid("model"), json={"id": "model_5906255e-0a3d-4fef-8653-8d41911264fb"})
        m.post(rest_helper.url_post_stat(pipeline_instance_id), json={})

        # Test Python channel
        mlops.init(ctx=None, mlops_mode=MLOpsMode.AGENT)
        published_model = mlops.Model(name="dtr_mlops_model",
                                      model_format=ModelFormat.SPARKML,
                                      description="model of decision tree regression with explainability")
        published_model.feature_importance(model=FinalModel, feature_names=FinalModel.feature_names,
                                           num_significant_features=num_significant_features)
        mlops.done()
        # TODO: ADD LATER Test PySpark channel


class FinalModel:
    feature_importances_ = [0.2, 0.3, 0.05, 0.4, 0.2, 0.8, 0.9, 0.02, 0.003, 0.5]
    feature_names = ['distance', 'pressure', 'altitude', 'temperature', 'PredictionHistogram',
                     'pipelinestat.averageDistanceToClusters', 'dataheatmap',
                     'pipelinestat.count', 'pipelinestat.WSSER',
                     'modelstats.distanceMatrixStat']


def test_publish_model_rest():
    with requests_mock.mock() as m:
        rh = MlOpsRestFactory().get_rest_helper(MLOpsMode.AGENT, mlops_server="localhost", mlops_port="4567")

        model_id = "model_5906255e-0a3d-4fef-8653-8d41911264fb"

        m.post('http://localhost:4567/models', json=model_id)
        m.get(rh.url_get_uuid("model"), json={"id": model_id})

        ion = ION()
        ion.id = "bdc2ee10-767c-4524-ba72-8268a3894bff"

        mh = ModelHelper(rest_helper=rh, ion=ion, stats_helper=None)

        model_data = "MODEL_DATA"
        model = mh.create_model(name="my model", model_format=ModelFormat.TEXT, description="test model")

        model_file = os.path.join(os.path.sep, "tmp", str(uuid.uuid4()))
        f = open(model_file, 'w')
        f.write(model_data)
        f.close()

        model.set_model_path(model_file)

        my_id = mh.publish_model(model, None)
        os.remove(model_file)

        assert (model_id == my_id)

        rh.done()
