
from parallelm.mlops.logger_factory import logger_factory


class BaseObj(object):
    """
    A base object for classes in mlops.
    Sets the logger as self._logger and exposes the _error and _info methods
    """

    def __init__(self, logger_name):
        self._logger = logger_factory.get_logger(logger_name)

    def _error(self, msg):
        self._logger.error(msg)

    def _warn(self, msg):
        self._logger.warn(msg)

    def _info(self, msg):
        self._logger.info(msg)

    def _debug(self, msg):
        self._logger.debug(msg)
