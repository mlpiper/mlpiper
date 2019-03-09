import json
import time
from parallelm.mlops.stats_category import StatTables
from parallelm.protobuf import InfoType_pb2


class MLOpsStat:
    """
    Contains the members of mlops stats
    """

    def __init__(self,
                 name=None,
                 stat_type=None,
                 data=None,
                 graph_type=None,
                 mode=None,
                 timestamp_ns=None,
                 should_transfer=False,
                 transfer_type=None,
                 stat_table=StatTables.USER_DEFINED,
                 string_data=None,
                 json_data_dump=None,
                 model_id=None):
        self.name = name
        self.stat_type = stat_type
        self.should_transfer = should_transfer
        self.transfer_type = transfer_type
        self.data_json = data
        self.graph_type = graph_type
        self.mode = mode
        self.timestamp_ns = timestamp_ns if timestamp_ns else int(time.time() * 1e+9)  # nanoseconds
        self.stat_table = stat_table
        self.string_data = string_data
        self.json_data_dump = json_data_dump
        self.model_id = model_id

    def data_to_json(self):
        if self.json_data_dump is not None:
            return self.json_data_dump
        return json.dumps(self.data_json)

    def to_json(self):
        d = {
            "Name": self.name,
            "Type": InfoType_pb2.InfoType.Name(self.stat_type),
            "Value": {
                "Data": self.data_json,
                "GraphType": self.graph_type,
                "Mode": self.mode,
                "Timestamp": self.timestamp_ns,
                "ShouldTransfer": self.should_transfer,
                "TransferType": self.transfer_type
            }
        }
        return json.dumps(d)

    def to_semi_json(self):
        sss = self.data_to_json()

        # if model_id is None, it will be correctly inserted as null into json,
        # and as NULL into DB
        value_dict = {
            "name": self.stat_table,
            "data": sss,
            "graphType": self.graph_type,
            "mode": self.mode,
            "timestamp": self.timestamp_ns,
            "type": InfoType_pb2.InfoType.Name(self.stat_type),
            "modelId": self.model_id
        }

        d = {
            "name": self.name,
            "value": json.dumps(value_dict)
        }
        return json.dumps(d)

    def to_csv_line(self):
        line = "{}, {}, {}, {}, {} ".format(self.timestamp_ns, self.name, self.graph_type, self.mode, self.data_json)
        return line

    def timestamp_ns_str(self):
        return str(self.timestamp_ns)
