from abc import abstractmethod
from abc import ABCMeta
from enum import Enum


class MLObjectType(Enum):
    MODEL = "model"


class MLObject:
    # This is supported by Python 2 and 3
    __metaclass__ = ABCMeta
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

    @abstractmethod
    def _get_object_type(self):
        pass

    @abstractmethod
    def __str__(self):
        pass
