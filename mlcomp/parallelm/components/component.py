import abc
from parallelm.common.base import Base


class Component(Base):
    def __init__(self, ml_engine):
        __metaclass__ = abc.ABCMeta  # Indicates an abstract class
        super(Component, self).__init__(ml_engine.get_engine_logger(self.logger_name()))
        self._ml_engine = ml_engine
        self._logger.debug("Creating pipeline component: " + self.name())
        self._params = None

    def configure(self, params):
        self._logger.info("Configure component with input params, name: {}, params: {}"
                          .format(self.name(), params))
        self._params = params

    @abc.abstractmethod
    def materialize(self, parent_data_objs):
        pass

    @abc.abstractmethod
    def _validate_output(self, objs):
        pass

    @abc.abstractmethod
    def _post_validation(self, objs):
        pass
