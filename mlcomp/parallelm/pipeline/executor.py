import json
import os
import six
import sys
import traceback

from parallelm.common.base import Base
from parallelm.common.mlcomp_exception import MLCompException
from parallelm.pipeline import components_desc
from parallelm.pipeline import json_fields
from parallelm.pipeline.dag import Dag
from parallelm.pipeline.data_type import EngineType
from parallelm.pipeline.component_language import ComponentLanguage
from parallelm.pipeline.executor_config import ExecutorConfig, MLCOMP_JAR_ARG, SPARK_JARS_ARG, SPARK_JARS_ENV_VAR

mlops_loaded = False
try:
    from parallelm.mlops import mlops
    from parallelm.mlops.common.string_ops import mask_passwords
    mlops_loaded = True
except ImportError as e:
    print("Note: was not able to import mlops: " + str(e))
    pass  # Designed for tests

class ExecutorException(Exception):
    pass

class Executor(Base):
    def __init__(self, args=None):
        super(Executor, self).__init__()

        self._args = args
        self._pipeline_file = None
        self._json_pipeline = None
        self._run_locally = False
        self._ml_engine = None
        self._pipeline = None
        self._mlcomp_jar = None
        self._use_color = True

        if args:
            self._json_pipeline = getattr(args, "pipeline", None)
            self._pipeline_file = getattr(args, "pipeline_file", None)
            self._run_locally = getattr(args, "run_locally", False)
            self._mlcomp_jar = getattr(args, MLCOMP_JAR_ARG, None)
            self._spark_jars = getattr(args, SPARK_JARS_ARG, None)
        else:
            self._spark_jars = os.environ.get(SPARK_JARS_ENV_VAR, None)

    def pipeline_file(self, pipeline_file):
        self._pipeline_file = pipeline_file
        return self

    def mlcomp_jar(self, mlcomp_jar):
        self._mlcomp_jar = mlcomp_jar
        return self

    def use_color(self, use_color):
        self._use_color = use_color
        return self

    @staticmethod
    def handle(args):
        Executor(args).go()

    @staticmethod
    def handle_deps(args):
        Executor(args).print_component_deps()

    def print_component_deps(self):
        """
        Printout all the python dependencies for a given pipeline. The dependencies are taken
        from all the component's metadata description files
        """
        all_deps = self.all_component_dependencies(self._args.lang)
        if all_deps:
            for dep in sorted(all_deps):
                print(dep)
        else:
            print("No dependencies were found for '{}'!".format(self._args.lang))

    def all_py_component_dependencies(self):
        return self.all_component_dependencies(ComponentLanguage.PYTHON)

    def all_r_component_dependencies(self):
        return self.all_component_dependencies(ComponentLanguage.R)

    def all_component_dependencies(self, lang):
        accumulated_py_deps = set()

        comps_desc_list = components_desc.ComponentsDesc().load()
        for comp_desc in comps_desc_list:
            if comp_desc[json_fields.PIPELINE_LANGUAGE_FIELD] == lang:
                deps = comp_desc.get(json_fields.COMPONENT_DESC_PYTHON_DEPS, None)
                if deps:
                    accumulated_py_deps.update(deps)

        return accumulated_py_deps

    def _parse_exit_code(self, code):
        # in case of calls like exit("some_string")
        return code if isinstance(code, six.integer_types) else 1

    def go(self):
        """
        Actual execution phase
        """

        self._logger.debug("Executor.go()")
        pipeline = self._load_pipeline()

        try:
            self._init_ml_engine(pipeline)

            comps_desc_list = components_desc.ComponentsDesc(self._ml_engine).load()
            self._logger.debug("comp_desc: {}".format(comps_desc_list))
            dag = Dag(pipeline, comps_desc_list, self._ml_engine).use_color(self._use_color)

            # Flush stdout so the logs looks a bit in order
            sys.stdout.flush()

            system_conf = pipeline[json_fields.PIPELINE_SYSTEM_CONFIG_FIELD]
            if dag.is_stand_alone:
                dag.run_single_component_pipeline(system_conf, self._ml_engine)
            else:
                dag.run_connected_pipeline(system_conf, self._ml_engine)
        # This except is intended to catch exit() calls from components.
        # Do not use exit() in mlpiper code.
        except SystemExit as e:
            code = self._parse_exit_code(e.code)
            error_message = "Pipeline called exit(), with code: {}".format(e.code)
            traceback_message = traceback.format_exc()
            if code != 0:
                self._logger.error("{}\n{}".format(error_message, traceback_message))
                # For Py2 put traceback into the exception message
                if sys.version_info[0] == 2:
                    error_message = "{}\n{}".format(error_message, traceback.format_exc())
                raise ExecutorException(error_message)
            else:
                self._logger.warn(error_message)
        finally:
            sys.stdout.flush()
            self._logger.info("Done running pipeline (in finally block)")
            self._cleanup_on_exist()
            print("End of go")

    def _init_ml_engine(self, pipeline):
        engine_type = pipeline[json_fields.PIPELINE_ENGINE_TYPE_FIELD]
        self._logger.info("Engine type: {}".format(engine_type))
        if engine_type == EngineType.PY_SPARK:
            from parallelm.ml_engine.py_spark_engine import PySparkEngine

            self._ml_engine = PySparkEngine(pipeline[json_fields.PIPELINE_NAME_FIELD], self._run_locally, self._spark_jars)
            self.set_logger(self._ml_engine.get_engine_logger(self.logger_name()))
            if mlops_loaded:
                mlops.init(self._ml_engine.context)

        elif engine_type == EngineType.GENERIC:
            from parallelm.ml_engine.python_engine import PythonEngine

            self._logger.info("Using python engine")
            self._ml_engine = PythonEngine(pipeline[json_fields.PIPELINE_NAME_FIELD], self._mlcomp_jar)
            self.set_logger(self._ml_engine.get_engine_logger(self.logger_name()))
            if mlops_loaded:
                # This initialization applies only to Python components and not to components
                # that are written in other languages (.e.g R). The reason for that is that
                # those components are executed within different process and thus need to
                # load and init the mlops library separately.
                mlops.init()

        elif engine_type == EngineType.REST_MODEL_SERVING:
            from parallelm.ml_engine.rest_model_serving_engine import RestModelServingEngine

            self._logger.info("Using REST Model Serving engine")
            self._ml_engine = RestModelServingEngine(pipeline[json_fields.PIPELINE_NAME_FIELD], self._mlcomp_jar)
            self.set_logger(self._ml_engine.get_engine_logger(self.logger_name()))
            if mlops_loaded:
                # This initialization applies only to Python components and not to components
                # that are written in other languages (.e.g R). The reason for that is that
                # those components are executed within different process and thus need to
                # load and init the mlops library separately.
                mlops.init()

        else:
            raise MLCompException("Engine type is not supported by the Python execution engine! engineType: " +
                            engine_type)

        if mlops_loaded:
            self._ml_engine.run(mlops, pipeline)

    def _cleanup_on_exist(self):
        if self._ml_engine:
            self._ml_engine.stop()

        if mlops_loaded and mlops.init_called:
            mlops.done()

        if self._ml_engine:
            self._ml_engine.cleanup()

    def _start_spark_session(self, name):
        # Doing the import here inorder not to require pyspark even if spark is not used
        if False:
            from pyspark.sql import SparkSession
        spark_session = SparkSession.builder.appName(name)
        if self._run_locally:
            spark_session.master("local[*]")

        return spark_session.getOrCreate()

    def _load_pipeline(self):
        if self._pipeline:
            return self._pipeline

        if self._json_pipeline:
            self._pipeline = json.loads(self._json_pipeline)
        elif self._pipeline_file:
            self._pipeline = json.load(self._pipeline_file)

        # Validations
        if json_fields.PIPELINE_PIPE_FIELD not in self._pipeline:
            raise MLCompException("Pipeline does not contain any component! pipeline=" + str(self._pipeline))

        if mlops_loaded:
            pipeline_str = mask_passwords(str(self._pipeline))
        else:
            pipeline_str = str(self._pipeline)
        self._logger.debug("Pipeline: " + pipeline_str)

        return self._pipeline
