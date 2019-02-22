import os
import json
import time
import logging

import pandas as pd

from parallelm.mlops.mlops_env_constants import MLOpsEnvConstants
from parallelm.protobuf.ReflexEvent_pb2 import ReflexEvent
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.events.event import Event
from parallelm.mlops.events.event_filter import EventFilter
from parallelm.mlops.constants import DataframeColNames
from parallelm.mlops.mlops_mode import MLOpsMode


class EventBroker(object):

    PipelineInstanceIdColumnName = "pipelineInstanceId"

    class Query:
        def __init__(self, query):
            self._query = query
            self._where_cond_added = False

        def add_condition(self, where_cond):
            if not self._where_cond_added:
                self._where_cond_added = True
                self._query += ' WHERE '
            else:
                self._query += ' AND '

            self._query += where_cond

        def get(self):
            return self._query

    def __init__(self, mlops_ctx, mlops_channel):
        self._logger = logging.getLogger(__name__)
        self._mlops_ctx = mlops_ctx
        self._mlops_channel = mlops_channel

    def _build_query(self, event_filter):

        query = EventBroker.Query('SELECT * from "mlobject"')

        query.add_condition('"alert" != 0')

        if event_filter.ion_name:
            query.add_condition('"ionName" = \'{}\''.format(event_filter.ion_name))

        if event_filter.ion_inst_id:
            query.add_condition('"ionId" = \'{}\''.format(event_filter.ion_inst_id))

        if event_filter.pipeline_inst_id:
            query.add_condition('"pipelineInstanceId" = \'{}\''.format(event_filter.pipeline_inst_id))

        if event_filter.agent_host:
            query.add_condition('"host" = \'{}\''.format(event_filter.agent_host))

        if event_filter.time_window_start:
            query.add_condition('"created" >= \'{}\''.format(event_filter.time_window_start))

        if event_filter.time_window_end:
            query.add_condition('"created" <= \'{}\''.format(event_filter.time_window_end))

        return query

    def _add_node_and_agent_cols(self, df):
        self._logger.info("Adding node")
        self._logger.info(df)
        if len(df) == 0:
            return
        pipe_inst_id_list = df[EventBroker.PipelineInstanceIdColumnName].tolist()
        nodes_list = []

        ion = self._mlops_ctx.ion()
        for pipe_inst in pipe_inst_id_list:
            if pipe_inst in ion.node_by_pipe_inst_id:
                node = ion.node_by_pipe_inst_id[pipe_inst]
                nodes_list.append(node.name)
            else:
                nodes_list.append('N/A')

        df[DataframeColNames.ION_NODE] = nodes_list
        return df

    def get_events(self, event_filter):
        if not isinstance(event_filter, EventFilter):
            raise MLOpsException("event_filter argument is not of type EventFilter")

        query = self._build_query(event_filter)
        self._logger.info(query.get())

        events_list_json = self._mlops_ctx.rest_helper().get_alerts(event_filter.to_query_dict())

        df = pd.DataFrame(events_list_json)

        df = self._add_node_and_agent_cols(df)
        return df

    def send_event(self, event_obj):

        if not isinstance(event_obj, Event):
            raise MLOpsException("Event object must be an instance of Event class")

        evt = ReflexEvent()
        evt.eventType = event_obj.type
        evt.eventLabel = event_obj.label
        evt.isAlert = event_obj.is_alert
        evt.data = self._event_data_as_json(int(time.time() * 1e3),
                                            event_obj.type, event_obj.label, event_obj.description, event_obj.data)

        self._logger.info("Sending alert: {}".format(evt))
        self._mlops_channel.event(evt)

    def _event_data_as_json(self, timestamp_ms, alert_type, title, desc, data):

        # TODO: we need to not use the MLOpsEnvConstants here, but to access the mlops_ctx (we need to define it
        # TODO: as singleton too

        if self._mlops_ctx._mode == MLOpsMode.STAND_ALONE:
            agent = "localhost"
        else:
            agent = os.environ[MLOpsEnvConstants.MLOPS_AGENT_PUBLIC_ADDRESS]

        json_data = json.dumps({
            "agent": agent,
            "timestamp_ms": timestamp_ms,
            "alert_type": alert_type,
            "title": title,
            "description": desc,
            "data": data})
        return json_data.encode()
