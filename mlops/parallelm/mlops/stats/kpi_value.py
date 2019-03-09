import six
import time

from parallelm.mlops.stats.mlops_stat_getter import MLOpsStatGetter
from parallelm.mlops.stats.mlops_stat import MLOpsStat
from parallelm.mlops.stats_category import StatGraphType, StatsMode
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats.value_formatter import ValueFormatter
from parallelm.protobuf import InfoType_pb2
import copy


class KpiValue(MLOpsStatGetter):
    """ A container for KPI value"""
    DEFAULT_NAME = "Kpi"
    TIME_SEC = "second"
    TIME_MSEC = "millisecond"
    TIME_NSEC = "nanosecond"

    def __init__(self, name, value, timestamp=None, units=None):
        self._name = name if name else KpiValue.DEFAULT_NAME
        self._value = copy.deepcopy(value)
        self._timestamp_ns = self._timestamp_as_nanoseconds(timestamp, units)
        self._units = units

    def _timestamp_as_nanoseconds(self, timestamp, units):
        if not timestamp:
            timestamp = time.time()
            units = KpiValue.TIME_SEC
        elif isinstance(timestamp, six.string_types):
            timestamp = float(timestamp)

        # Default timestamp units are nanoseconds
        if units:
            if units == KpiValue.TIME_SEC:
                timestamp = timestamp * 1e+9
            elif units == KpiValue.TIME_MSEC:
                timestamp = timestamp * 1e+6
            elif units != KpiValue.TIME_NSEC:
                raise MLOpsException("Invalid timestamp units! unit: {}, should be one of: {}".format(
                    units, [KpiValue.TIME_SEC, KpiValue.TIME_MSEC, KpiValue.TIME_NSEC]))

        return timestamp

    def get_mlops_stat(self, model_id):
        graph_type = StatGraphType.LINEGRAPH
        formatted_value = ValueFormatter(self._name, self._value, graph_type).value()

        mlops_stat = MLOpsStat(name=self._name,
                               stat_type=InfoType_pb2.KPI,
                               graph_type=graph_type,
                               mode=StatsMode.TimeSeries,
                               data=formatted_value,
                               timestamp_ns=self._timestamp_ns,
                               model_id=model_id)
        return mlops_stat

