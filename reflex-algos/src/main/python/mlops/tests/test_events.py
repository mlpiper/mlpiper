import requests_mock

from parallelm.mlops.mlops_mode import MLOpsMode
from parallelm.mlops.events.event_broker import EventBroker
from parallelm.mlops.events.event_filter import EventFilter
from parallelm.mlops.mlops_rest_factory import MlOpsRestFactory

from ion_test_helper import build_ion_ctx, ION1


class AlertsInfo:
    ALERT_0_ID = "0"
    ALERT_1_ID = "1"
    ALERT_2_ID = "2"
    ALERT_3_ID = "3"


alerts_list = [
    {
        u'ionName': u'aaa',
        u'raiseAlert': True,
        u'description': u'Data alert (anomaly) reported from mlops PySpark [agent: 127.0.0.1]',
        u'pipelineInstanceId': ION1.PIPELINE_INST_ID_0,
        u'sequence': 1,
        u'workflowRunId': ION1.ION_ID,
        u'eventType': u'GenericDataAlert',
        u'clearedTimestamp': 0,
        u'created': 100,
        u'name': u'event-1',
        u'createdTimestamp': 100,
        u'host': u'localhost',
        u'msgType': u'UNKNOWN',
        u'deletedTimestamp': 0,
        u'type': u'GenericDataAlert',
        u'id': AlertsInfo.ALERT_0_ID,
        u'modelId': u'6e52e9db-a74a-4bb5-a253-c64a32e85bd8',
    },
    {
        u'ionName': u'aaa',
        u'raiseAlert': True,
        u'description': u'Health alert reported from mlops PySpark [agent: 127.0.0.1]',
        u'pipelineInstanceId': ION1.PIPELINE_INST_ID_1,
        u'sequence': 2,
        u'workflowRunId': ION1.ION_ID,
        u'eventType': u'GenericHealthAlert',
        u'clearedTimestamp': 0,
        u'created': 102,
        u'name': u'event-2',
        u'createdTimestamp': 102,
        u'host': u'localhost',
        u'msgType': u'UNKNOWN',
        u'deletedTimestamp': 0,
        u'type': u'GenericHeathaAlert',
        u'id': AlertsInfo.ALERT_1_ID,
        u'modelId': u'6e52e9db-a74a-4bb5-a253-c64a32e85bd8',
    }
]


def test_all_alerts():
    mlops_ctx = build_ion_ctx()

    with requests_mock.mock() as m:
        m.get('http://localhost:3456/events', json=alerts_list)

        rest_helper = MlOpsRestFactory().get_rest_helper(MLOpsMode.AGENT)
        event_helper = EventBroker(mlops_ctx, None)

        ef = EventFilter()

        alerts = event_helper.get_events(ef)

        assert len(alerts) == 2
        alert_df = alerts[alerts.id == AlertsInfo.ALERT_0_ID]
        assert alert_df.iloc[0]["node"] == ION1.NODE_0_ID
        rest_helper.done()
