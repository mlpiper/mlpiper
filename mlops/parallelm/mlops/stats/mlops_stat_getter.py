import abc
from future.utils import with_metaclass


class MLOpsStatGetter(with_metaclass(abc.ABCMeta, object)):
    """
    This abstract class requires that the class implementing it should be able to provide MLOpsStats via the
    get_mlops_stat method
    """

    @abc.abstractmethod
    def get_mlops_stat(self, model_id):
        pass
