import logging

from parallelm.mlops.singelton import Singleton


@Singleton
class LoggerFactory(object):
    """
    Provide a way to get the logger.
    When running under pyspark context, the regular python logger does not work. The
    solution is to generate the logger via the java context.
    """

    def __init__(self):
        self._logger_provider_func = logging.getLogger

    def get_logger(self, name):
        return self._logger_provider_func(name)

    def set_logger_provider_func(self, func):
        self._logger_provider_func = func


logger_factory = LoggerFactory.Instance()
