import abc
from enum import Enum
from future.utils import with_metaclass


class MLObjectType(Enum):
    MODEL = "model"


class MLObject(with_metaclass(abc.ABCMeta, object)):
    """
    Base class for the all MLObjects
    """

    def __init__(self, rest_helper, id=None):
        ml_object_type = self._get_object_type()
        if not isinstance(ml_object_type, MLObjectType):
            raise Exception("type: {} is not an instance of MLObjectType".format(ml_object_type))
        self._id = id if id else rest_helper.get_uuid(ml_object_type.value)
        self._rest_helper = rest_helper

    def get_id(self):
        return self._id

    @abc.abstractmethod
    def _get_object_type(self):
        pass

    @abc.abstractmethod
    def __str__(self):
        pass
