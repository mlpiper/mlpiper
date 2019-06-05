import glob
import math
import os
import psutil
import sys
import pprint

from py4j.java_gateway import JavaGateway
from py4j.java_gateway import GatewayParameters, CallbackServerParameters, launch_gateway
from py4j.protocol import Py4JJavaError

from parallelm.common.mlcomp_exception import MLCompException
from parallelm.common.process_monitor import ProcessMonitor
from parallelm.common.byte_conv import ByteConv
from parallelm.pipeline.component_runner.component_runner import ComponentRunner

mlops_loaded = False
try:
    from parallelm.mlops import mlops
    mlops_loaded = True
except ImportError as e:
    print("Note: was not able to import mlops: " + str(e))
    pass  # Designed for tests


class MLOpsPY4JWrapper(object):

    def setStat(self, stat_name, stat_value):
        if mlops_loaded:
            mlops.set_stat(stat_name, stat_value)
        else:
            print("no-mlops: stat {} {}".format(stat_name, stat_value))

    def isTestMode(self):
        if mlops_loaded:
            return mlops.test_mode
        else:
            print("mlops is not loaded: test_mode is not available")
        return False

    class Java:
        implements = ["com.parallelm.mlcomp.MLOps"]


class JavaConnectedComponentRunner(ComponentRunner):

    JAVA_PROGRAM = "java"
    SLEEP_INTERVAL_AFTER_RUNNING_JVM = 2
    ENTRY_POINT_CLASS = "com.parallelm.mlcomp.ComponentEntryPoint"

    def __init__(self, ml_engine, dag_node, mlcomp_jar):
        super(JavaConnectedComponentRunner, self).__init__(ml_engine, dag_node)

        if not mlcomp_jar or not os.path.exists(mlcomp_jar):
            raise Exception("mlcomp_jar does not exists: {}".format(mlcomp_jar))
        self._mlcomp_jar = mlcomp_jar

    def run(self, parent_data_objs):

        # Run the java py4j entry point
        comp_dir = self._dag_node.comp_root_path()
        self._logger.info("comp_dir: {}".format(comp_dir))

        jar_files = glob.glob(os.path.join(comp_dir, "*.jar"))
        self._logger.info("Java classpath files: {}".format(jar_files))
        component_class = self._dag_node.comp_class()

        java_jars = [self._mlcomp_jar] + jar_files
        class_path = ":".join(java_jars)
        java_gateway = None
        all_ok = False
        monitor_proc = None

        try:
            total_phys_mem_size_mb = ByteConv.from_bytes(psutil.virtual_memory().total).mbytes
            jvm_heap_size_option = "-Xmx{}m".format(int(math.ceil(total_phys_mem_size_mb)))

            java_opts = [jvm_heap_size_option]
            self._logger.info("JVM options: {}".format(java_opts))

            # Note: the jarpath is set to be the path to the mlcomp jar since the launch_gateway code is checking
            #       for the existence of the jar. The py4j jar is packed inside the mlcomp jar.
            java_port = launch_gateway(port=0,
                                       javaopts=java_opts,
                                       die_on_exit=True,
                                       jarpath=self._mlcomp_jar,
                                       classpath=class_path,
                                       redirect_stdout=sys.stdout,
                                       redirect_stderr=sys.stderr)

            java_gateway = JavaGateway(
                gateway_parameters=GatewayParameters(port=java_port),
                callback_server_parameters=CallbackServerParameters(port=0),
                python_server_entry_point=MLOpsPY4JWrapper()
            )

            python_port = java_gateway.get_callback_server().get_listening_port()
            self._logger.debug("Python port: {}".format(python_port))

            java_gateway.java_gateway_server.resetCallbackClient(
                java_gateway.java_gateway_server.getCallbackClient().getAddress(),
                python_port)

            mlops_wrapper = MLOpsPY4JWrapper()
            entry_point = java_gateway.jvm.com.parallelm.mlcomp.ComponentEntryPoint(component_class)
            component_via_py4j = entry_point.getComponent()
            component_via_py4j.setMLOps(mlops_wrapper)

            # Configure
            m = java_gateway.jvm.java.util.HashMap()
            for key in self._params.keys():
                # py4j does not handle nested structures. So the configs which is a dict will not be passed to the java
                # layer now.
                if isinstance(self._params[key], dict):
                    continue
                m[key] = self._params[key]

            component_via_py4j.configure(m)

            # Materialized
            l = java_gateway.jvm.java.util.ArrayList()
            for obj in parent_data_objs:
                l.append(obj)
                self._logger.info("Parent obj: {} type {}".format(obj, type(obj)))
            self._logger.info("Parent objs: {}".format(l))

            if mlops_loaded:
                monitor_proc = ProcessMonitor(mlops, self._ml_engine)
                monitor_proc.start()

            py4j_out_objs = component_via_py4j.materialize(l)

            self._logger.debug(type(py4j_out_objs))
            self._logger.debug(len(py4j_out_objs))

            python_out_objs = []
            for obj in py4j_out_objs:
                self._logger.debug("Obj:")
                self._logger.debug(obj)
                python_out_objs.append(obj)
            self._logger.info("Done running of materialize and getting output objects")
            all_ok = True
        except Py4JJavaError as e:
            self._logger.error("Error in java code: {}".format(e))
            raise MLCompException(str(e))
        except Exception as e:
            self._logger.error("General error: {}".format(e))
            raise MLCompException(str(e))
        finally:
            self._logger.info("In finally block: all_ok {}".format(all_ok))
            if java_gateway:
                java_gateway.close_callback_server()
                java_gateway.shutdown()

            if mlops_loaded and monitor_proc:
                monitor_proc.stop_gracefully()

        return python_out_objs
