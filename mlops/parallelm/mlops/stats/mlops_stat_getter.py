import abc


class MLOpsStatGetter:
    """
    This abstract class requires that the class implementing it should be able to provide MLOpsStats via the
    get_mlops_stat method
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_mlops_stat(self, model_id):
        pass
