import pickle
import binascii

from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats_category import StatGraphType, StatsMode
from parallelm.mlops.stats.mlops_stat import MLOpsStat

from parallelm.mlops.stats.mlops_stat_getter import MLOpsStatGetter
from parallelm.protobuf import InfoType_pb2


class Opaque(MLOpsStatGetter):
    """
    The Opaque statistic type can be used to generate, store, and retrieve arbitrary statistics data.
    Since this type is opaque by definition, it is not automatically visualized by the MLOps UI.

    :Example:

    >>> obj = {"1": "aaaaaa", "2": 33}
    >>> opq = Opaque().name("opaque stat name").data(obj)
    >>> mlops.set_stat(opq)

    In this example, an arbitrary object is created.
    In the next line, an Opaque object is created, its name is set to "opaque stat name",
    and its data is set to the value of obj.
    In the final line, the Opaque object is reported to MLOps.
    """
    def __init__(self):
        self._name = "Opaque"
        self._data = None

    def name(self, name):
        """
        Set the name of the object

        :param name: object name
        :return: self
        """
        self._name = name
        return self

    def data(self, obj):
        """
        Set the data of the object

        :param obj: data
        :return: self
        """

        self._data = binascii.hexlify(pickle.dumps(obj)).decode('utf-8')

        return self

    def _to_dict(self):
        dd = {self._name: self._data}
        return dd

    def get_mlops_stat(self, model_id):

        if self._data is None:
            raise MLOpsException("There is no data in opaque stat")

        data = self._to_dict()

        mlops_stat = MLOpsStat(name=self._name,
                               stat_type=InfoType_pb2.General,
                               graph_type=StatGraphType.OPAQUE,
                               mode=StatsMode.Instant,
                               data=data,
                               model_id=model_id)
        return mlops_stat
