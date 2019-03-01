import pickle
import binascii

from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats_category import StatGraphType, StatsMode
from parallelm.mlops.stats.mlops_stat import MLOpsStat

from parallelm.mlops.stats.mlops_stat_getter import MLOpsStatGetter
from parallelm.protobuf import InfoType_pb2


class HTML(MLOpsStatGetter):
    """
    Store and render html content in ui
    """
    def __init__(self):
        self._name = "HTML"
        self._data = None

    def name(self, name):
        """
        Set the name of the object

        :param name: object name
        :return: self
        """
        self._name = name
        return self

    def data(self, html):
        """
        Set the data of the object

        :param obj: data
        :return: self
        """

        self._data = html.decode('utf-8')

        return self

    def _to_dict(self):
        dd = {self._name: self._data}
        return dd

    def get_mlops_stat(self, model_id):

        if self._data is None:
            raise MLOpsException("There is no data in html object")

        mlops_stat = MLOpsStat(name=self._name,
                               stat_type=InfoType_pb2.General,
                               graph_type=StatGraphType.HTML,
                               mode=StatsMode.Instant,
                               json_data_dump=self._data,
                               model_id=model_id)
        return mlops_stat
