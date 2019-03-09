"""
The RESTfulComponent class is used to create RESTful model serving components.
It is an abstract class that is supposed to be extended and the sub-class is
supposed to implement its abstract methods.

The class provide the core functionality of the model restful serving, which
includes setup and preparation of the environment, configurations as well as
executing of required services (.e.g uwsig, nginx)
"""

import abc
import os
import tempfile
import time
import logging

from parallelm.pipeline import java_mapping
from parallelm.model.model_env import ModelEnv
from parallelm.ml_engine.rest_model_serving_engine import RestModelServingEngine
from parallelm.common import constants
from parallelm.components import parameter
from parallelm.common.mlcomp_exception import MLCompException
from parallelm.components.connectable_component import ConnectableComponent
from parallelm.components.restful.constants import SharedConstants, ComponentConstants, UwsgiConstants, NginxConstants
from parallelm.components.restful.metric import Metric
from parallelm.components.restful.uwsgi_broker import UwsgiBroker
from parallelm.components.restful.nginx_broker import NginxBroker


class RESTfulComponent(ConnectableComponent):
    def __init__(self, engine):
        super(RESTfulComponent, self).__init__(engine if engine else RestModelServingEngine("uwsgi-context"))
        self._dry_run = False
        self._wsgi_broker = None
        self._nginx_broker = None
        self._wid = None

    def set_wid(self, wid):
        self._wid = wid

    def get_wid(self):
        return self._wid

    def _validate_output(self, objs):
        pass

    def _post_validation(self, objs):
        pass

    def materialize(self, parent_data_objs):
        monitor_info = {UwsgiConstants.MONITOR_ERROR_KEY: None, UwsgiConstants.MONITOR_THREAD_KEY: None}
        self._setup(self._ml_engine.pipeline_name, monitor_info)
        self._wait_and_monitor_errors(monitor_info)

    def _setup(self, pipeline_name, monitor_info):
        target_path = tempfile.mkdtemp(dir=ComponentConstants.TMP_RESTFUL_ROOT,
                                       prefix=ComponentConstants.TMP_RESTFUL_DIR_PREFIX)
        os.chmod(target_path, 0o777)

        shared_conf = {
            SharedConstants.TARGET_PATH_KEY: target_path,
            SharedConstants.SOCK_FILENAME_KEY: UwsgiConstants.SOCK_FILENAME,
            SharedConstants.STATS_SOCK_FILENAME_KEY: UwsgiConstants.STATS_SOCK_FILENAME
        }

        log_format = self._params.get(ComponentConstants.LOG_FORMAT_KEY, ComponentConstants.DEFAULT_LOG_FORMAT)

        log_level_param = self._params.get(ComponentConstants.LOG_LEVEL_KEY, ComponentConstants.DEFAULT_LOG_LEVEL).lower()
        log_level = constants.LOG_LEVELS.get(log_level_param, logging.INFO)
        self._logger.debug("log_level_param: {}, log_level: {}, level_constants: {}"
                           .format(log_level_param, log_level, constants.LOG_LEVELS))

        stats_reporting_interval_sec = self._params.get(ComponentConstants.STATS_REPORTING_INTERVAL_SEC,
                                                        ComponentConstants.DEFAULT_STATS_REPORTING_INTERVAL_SEC)

        model_filepath_key = java_mapping.RESERVED_KEYS[ComponentConstants.INPUT_MODEL_TAG_NAME]
        self._params[model_filepath_key] = ModelEnv(self._params[model_filepath_key]).model_filepath

        uwsgi_entry_point_conf = {
            UwsgiConstants.RESTFUL_COMP_MODULE_KEY: self.__module__,
            UwsgiConstants.RESTFUL_COMP_CLS_KEY: self.__class__.__name__,
            ComponentConstants.LOG_FORMAT_KEY: log_format,
            ComponentConstants.LOG_LEVEL_KEY: log_level,
            ComponentConstants.STATS_REPORTING_INTERVAL_SEC: stats_reporting_interval_sec,
            UwsgiConstants.PARAMS_KEY: self._params,
            UwsgiConstants.PIPELINE_NAME_KEY: pipeline_name,
            UwsgiConstants.MODEL_PATH_KEY: self._params[model_filepath_key],
            ComponentConstants.UWSGI_DISABLE_LOGGING_KEY:
                parameter.str2bool(self._params.get(ComponentConstants.UWSGI_DISABLE_LOGGING_KEY,
                                                    ComponentConstants.DEFAULT_UWSGI_DISABLE_LOGGING)),
            ComponentConstants.METRICS_KEY: Metric.metrics()
        }
        self._logger.debug("uwsgi_entry_point_conf: {}".format(uwsgi_entry_point_conf))

        nginx_conf = {
            ComponentConstants.HOST_KEY: ComponentConstants.DEFAULT_HOST,
            ComponentConstants.PORT_KEY: self._params[ComponentConstants.PORT_KEY],
            NginxConstants.DISABLE_ACCESS_LOG_KEY: log_level != logging.DEBUG
        }
        self._logger.debug("nginx_conf: {}".format(nginx_conf))

        self._dry_run = parameter.str2bool(self._params.get(ComponentConstants.DRY_RUN_KEY,
                                                            ComponentConstants.DEFAULT_DRY_RUN))
        if self._dry_run:
            self._logger.warn("\n\n" + 80 * '#' + "\n" + 25 * " " + "Running in DRY RUN mode\n" + 80 * '#')

        self._dry_run = parameter.str2bool(self._params.get(ComponentConstants.DRY_RUN_KEY, ComponentConstants.DEFAULT_DRY_RUN))

        self._wsgi_broker = UwsgiBroker(self._ml_engine, self._dry_run) \
            .setup_and_run(shared_conf, uwsgi_entry_point_conf, monitor_info)

        self._nginx_broker = NginxBroker(self._ml_engine, self._dry_run) \
            .setup_and_run(shared_conf, nginx_conf)

    def _wait_and_monitor_errors(self, monitor_info):
        self._logger.info("Going to read model / stop events ... (kidding, going to sleep forever ...)")

        if not self._dry_run and monitor_info[UwsgiConstants.MONITOR_THREAD_KEY]:
            monitor_info[UwsgiConstants.MONITOR_THREAD_KEY].join()

            if monitor_info[UwsgiConstants.MONITOR_ERROR_KEY]:
                self._logger.error(monitor_info[UwsgiConstants.MONITOR_ERROR_KEY])
                self._nginx_broker.quit()
                self._wsgi_broker.quit()
                raise MLCompException(monitor_info[UwsgiConstants.MONITOR_ERROR_KEY])
        else:
            while True:
                time.sleep(3600*24*365)

    @abc.abstractmethod
    def load_model_callback(self, model_path, stream, version):
        """
        This abstract method is called whenever a new model is supposed to be loaded. The user is responsible
        to reload the model and start using it in any consequent predictions

        :param model_path: an absolute file path to the model
        """
        pass

    def _on_exit(self):
        cleanup_op = getattr(self, ComponentConstants.CLEANUP_CALLBACK_FUNC_NAME, None)
        if callable(cleanup_op):
            cleanup_op()
        else:
            self._logger.info("'{}' function is not defined by the restful child component!"
                              .format(ComponentConstants.CLEANUP_CALLBACK_FUNC_NAME))

    @classmethod
    def run(cls, port, model_path):
        """
        Enables the user to test his restful component in a standalone mode. It runs a local rest server that
        listens the given port of the 'localhost' (127.0.0.1) interface.

        Usually, it should be called under the __main__ scope:

            if __name__ == '__main__':
                MyRestfulComp.run(port=9999, model_path='/tmp/dummy-model.txt')

        :param port:  specifies the port to listen on.
        :param model_path:  a path in the local file system to a model file.
        """
        if not isinstance(port, int):
            print("Port argument should be integer! provided: {}, port: {}".format(type(port), port))
            return

        pipeline_name = "standalone-restful-app"
        UwsgiBroker.uwsgi_entry_point(cls(RestModelServingEngine(pipeline_name)), pipeline_name, model_path,
                                      within_uwsgi_context=False)

        UwsgiBroker._application.run(host='localhost', port=port)


if __name__ == '__main__':

    from parallelm.components.restful.flask_route import FlaskRoute

    class MyResfulComp(RESTfulComponent):
        def __init__(self, engine):
            super(RESTfulComponent, self).__init__(engine)

        def load_model(self, model_path):
            print("Model is reloading, path: {}".format(model_path))

        @FlaskRoute('/v1/predict')
        def predict_v1(self, url_params, form_params):
            print("url_params: {}".format(url_params))
            print("form_params: {}".format(form_params))

            status_code = 200
            dict_response = {'user': 'jon', 'age': 100}
            return (status_code, dict_response)

        @FlaskRoute('/v2/predict')
        def predict_v2(self, url_params, form_params):
            print("url_params: {}".format(url_params))
            print("form_params: {}".format(form_params))

            status_code = 200
            dict_response = {'user': 'jon2', 'age': 200}
            return (status_code, dict_response)

    MyResfulComp.run(9999, '/tmp/model_path/xxx.txt')
