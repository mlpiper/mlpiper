class EventFilter(object):
    def __init__(self):
        self.ion_name = None
        self.ion_inst_id = None
        self.pipeline_inst_id = None
        self.agent_host = None
        self.time_window_start = None
        self.time_window_end = None
        self.is_alert = None

    def to_query_dict(self):
        """
        Convert the filter to a dict where the keys are query params for the server
        :return: dict
        """
        query_dict = {}

        query_dict["startTime"] = self.time_window_start if self.time_window_start else -1
        query_dict["endTime"] = self.time_window_end if self.time_window_end else -1

        query_dict["ionInstanceId"] = self.ion_inst_id
        if self.is_alert is not None:
            query_dict["isAlert"] = self.is_alert
        return query_dict

    def __str__(self):
        return "ion_name: {} ion_inst: {} pipe: {} agent: {} time_window[{}, {}] is_alert: {}".format(
            self.ion_name,
            self.ion_inst_id,
            self.pipeline_inst_id,
            self.agent_host,
            self.time_window_start,
            self.time_window_end,
            self.is_alert)
