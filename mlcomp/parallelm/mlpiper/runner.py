import sys
import logging
import traceback

from parallelm.deputy.deps_install.py_package_installer import PyPackageInstaller
from parallelm.deputy.deps_install.r_package_installer import RPackageInstaller
from parallelm.pipeline.executor import Executor


class MLPipelineRunner(object):
    """
    Given a prepared pipeline, run the pipeline
    o Install missing packages
    o Fetch models (TBD)
    o Rune pipeline
    o Report status
    """

    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._pipeline_file = None
        self._mlcomp_jar = None

    def pipeline(self, pipeline_file):
        self._pipeline_file = pipeline_file
        return self

    def mlcomp_jar(self, mlcomp_jar):
        self._mlcomp_jar = mlcomp_jar
        return self

    def run(self):
        self._logger.info("Deputy starting")

        # TODO: move to a thread/separate process so we can track
        ret_val = 1
        try:
            pipeline_runner = Executor(args=None).pipeline_file(self._pipeline_file).mlcomp_jar(self._mlcomp_jar)

            py_deps = pipeline_runner.all_py_component_dependencies()
            if py_deps:
                PyPackageInstaller(py_deps).install()

            r_deps = pipeline_runner.all_r_component_dependencies()
            if r_deps:
                RPackageInstaller(r_deps).install()

            pipeline_runner.go()
            ret_val=0
        except Exception as e:
            self._logger.info("Got exception while running code: {}".format(e))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print("========= Error from code ==========")
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            print("========= Error from code ==========")
            ret_val = 1
        finally:
            self._logger.info("Deputy done - finally block")

        return ret_val
