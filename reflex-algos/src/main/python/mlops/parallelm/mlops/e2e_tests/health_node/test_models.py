
from __future__ import print_function

from datetime import datetime, timedelta

from parallelm.mlops import mlops as mlops
from parallelm.mlops.e2e_tests.e2e_constants import E2EConstants


def test_models():
    # Getting models:
    now = datetime.utcnow()
    last_hour = (now - timedelta(hours=1))

    print("MODEL-TEST")
    models = mlops.get_models_by_time(start_time=last_hour, end_time=now)
    print("models-df:\n", models)
    expected_model_data = bytes(E2EConstants.MODEL_CONTENT)
    expected_model_size = len(expected_model_data)
    print("Got {} models".format(len(models)))
    print(models)
    if len(models) >= 1:
        model_id = models.iloc[0]['id']

        models_size = list(set(models["modelSize"].tolist()))
        print("models_size {} ".format(models_size))
        print("expected_model_size: {}".format(expected_model_data))
        assert len(models_size) == 1 and models_size[0] == expected_model_size

        print("\n\nDownloading first model: {} {}\n".format(model_id, type(model_id)))
        model_df_by_id = mlops.get_model_by_id(model_id, download=True)

        assert len(model_df_by_id) == 1
        model_data = model_df_by_id.iloc[0]['data']
        print("Model data: [{}]".format(model_data))
        assert model_data == expected_model_data

    models = mlops.get_models_by_time(start_time=last_hour, end_time=now, download=True)
    if len(models) >= 1:
        model_data = models.iloc[0]['data']
        assert model_data == expected_model_data

    print("MODEL-TEST-END")


def test_model_stats():
    print("MODEL-STATS-TEST")
    now = datetime.utcnow()
    last_10_hour = (now - timedelta(hours=10))

    models_df = mlops.get_models_by_time(start_time=last_10_hour, end_time=now)
    print(models_df)

    if len(models_df) == 0:
        print("No models yet - skipping for now")
        return

    # Taking the last model generated
    tail_df = models_df.tail(1)
    print("tail_models_stat:{}".format(tail_df))
    model_id = tail_df.iloc[0]['id']
    print("model_id: [{}]".format(model_id))

    is_model_stats_reported_df = mlops.get_stats(name=E2EConstants.MODEL_STATS_REPORTED_STAT_NAME, mlapp_node=None, agent=None,
                                                 start_time=last_10_hour, end_time=now)

    # TODO: we need to somehow indicate that this test was actually skipped and not passing
    if len(is_model_stats_reported_df) > 0:
        # Only in this case we check the model statistics

        model_stats = mlops.get_data_distribution_stats(model_id)
        assert len(model_stats) > 0

        print("model_stats:\n{}".format(model_stats))

        # Getting the attributes reported in the histograms and comparing to the expected list of attributes
        existing_attributes_set = set(model_stats['stat_name'].tolist())
        diff_set = existing_attributes_set ^ E2EConstants.MODEL_STATS_EXPECTED_ATTR_SET
        print("existing attr: {}".format(existing_attributes_set))
        print("expected attr: {}".format(E2EConstants.MODEL_STATS_EXPECTED_ATTR_SET))
        print("diff_set: {}".format(diff_set))
        assert len(diff_set) == 0
