import logging

from parallelm.ml_engine.ml_engine import MLEngine


class PythonEngine(MLEngine):
    """
    Implementing the MLEngine API for a python engine.
    """
    def __init__(self, pipeline, mlcomp_jar=None, standalone=False):
        super(PythonEngine, self).__init__(pipeline, standalone)
        self._config = {
            "mlcomp_jar": mlcomp_jar
        }

        self.set_logger(self.get_engine_logger(self.logger_name()))

    def finalize(self):
        pass

    def cleanup(self):
        pass

    def get_engine_logger(self, name):
        return logging.getLogger(name)

    def _session(self):
        pass

    def _context(self):
        pass
