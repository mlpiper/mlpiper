import glob
import json
import os
from tempfile import mkstemp, mkdtemp
import shutil
import subprocess
import sys

from constants import PYTHON_COMPONENTS_PATH, JAVA_COMPONENTS_PATH

simple_pipelines_for_testing = [
    {
        "name": "Simple MLPiper runner test",
        "engineType": "Generic",
        "pipe": [
            {
                "name": "Source String",
                "id": 1,
                "type": "string-source",
                "parents": [],
                "arguments": {
                    "value": "Hello World: testing string source and sink"
                }
            },
            {
                "name": "Sink String",
                "id": 2,
                "type": "string-sink",
                "parents": [{"parent": 1, "output": 0}],
                "arguments": {
                    "expected-value": "Hello World: testing string source and sink",
                    "check-test-mode": True
                }
            }
        ]
    },
    {
        "name": "Simple MLPiper runner test (shared dir)",
        "engineType": "Generic",
        "pipe": [
            {
                "name": "Source String (Shared Dir)",
                "id": 1,
                "type": "string-source-shared-dir",
                "parents": [],
                "arguments": {
                    "value": "Hello World: testing string source and sink"
                }
            },
            {
                "name": "Sink String (Shared Dir)",
                "id": 2,
                "type": "string-sink-shared-dir",
                "parents": [{"parent": 1, "output": 0}],
                "arguments": {
                    "expected-value": "Hello World: testing string source and sink"
                }
            }
        ]
    }
]

model_src_sink_pipeline = {
    "name": "Sink/Src MLPiper runner test",
    "engineType": "Generic",
    "pipe": [
        {
            "name": "Test source model",
            "id": 1,
            "type": "test-predict-src",
            "parents": [],
            "arguments": {
                "exit_value": 0,
                "iter": 0
            }
        },
        {
            "name": "Test sink model",
            "id": 2,
            "type": "test-train-sink",
            "parents": [],
            "arguments": {
                "exit_value": 0,
                "iter": 0,
                "model_content": "Model 1"
            }
        }
    ]
}

deps_show_pipeline = {
    "name": "Deps Show Pipeline",
    "engineType": "Generic",
    "pipe": [
        {
            "name": "Test source model",
            "id": 1,
            "type": "test-component-deps",
            "parents": [],
            "arguments": {}
        }
    ]
}

parse_pipeline_with_unicode_symbol_in_component = {
    "name": "Parse Pipeline with unicode ",
    "engineType": "Generic",
    "pipe": [
        {
            "name": "Test source model",
            "id": 1,
            "type": "test-component-with-unicode",
            "parents": [],
            "arguments": {}
        }
    ]
}

java_connected_pipeline = {
    "name": "connected_with_multiple_jars_test",
    "engineType": "Generic",
    "pipe": [
        {
            "name": "Java",
            "id": 1,
            "type": "test-java-connected-multiple-jars",
            "parents": [],
            "arguments": {}
        }
    ]
}


class TestMLPiper:
    mlpiper_script = None
    mlcomp_jar = None
    egg_paths = []
    pipelines_to_test = []
    skip_cleanup = False

    @classmethod
    def setup_class(cls):
        mlcomp_root_path = os.path.join(os.path.dirname(__file__), "..")
        # os.chdir(mlcomp_root_path)
        # subprocess.check_call("make egg", shell=True)

        mlcomp_eggs = glob.glob(os.path.join(mlcomp_root_path, "dist", "*.egg"))
        TestMLPiper.egg_paths.extend(mlcomp_eggs)

        mlops_root_path = os.path.join(os.path.dirname(__file__), "../../mlops")
        # os.chdir(mlops_root_path)
        # subprocess.check_call("make egg", shell=True)

        mlops_eggs = glob.glob(os.path.join(mlops_root_path, "dist", "*2.*.egg" if sys.version_info[0] < 3 else "*3.*.egg"))
        TestMLPiper.egg_paths.extend(mlops_eggs)

        TestMLPiper.mlpiper_script = os.path.join(mlcomp_root_path, "bin", "mlpiper")
        print("mlpiper_script: {}".format(TestMLPiper.mlpiper_script))

        TestMLPiper.mlcomp_jar = os.path.join(mlcomp_root_path, "..", "reflex-common", "mlcomp", "target", "mlcomp.jar")

    @classmethod
    def teardown_class(cls):
        pass

    def setup_method(self, method):
        """ setup any state tied to the execution of the given method in a
        class.  setup_method is invoked for every test method of a class.
        """
        print("setup_method: {}".format(method))
        self._deployment_dir = None

    def teardown_method(self, method):
        """ teardown any state that was previously setup with a setup_method
        call.
        """
        print("teardown_method: {}".format(method))
        if self._deployment_dir and not TestMLPiper.skip_cleanup:
            shutil.rmtree(self._deployment_dir)

    @staticmethod
    def _next_pipeline():
        for pipeline in simple_pipelines_for_testing:
            try:
                _, pipeline_tmp_file = mkstemp(prefix='test_mlpiper_pipeline_', dir='/tmp')
                print("pipeline_tmp_file:", pipeline_tmp_file)
                with open(pipeline_tmp_file, 'w') as f:
                    json.dump(pipeline, f)
                yield pipeline_tmp_file
            finally:
                if pipeline_tmp_file:
                    os.remove(pipeline_tmp_file)

    def _exec_shell_cmd(self, cmd, err_msg):
        os.environ["PYTHONPATH"] = ":".join(TestMLPiper.egg_paths)

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, env=os.environ)
        (stdout, stderr) = p.communicate()

        print("stdout: {}".format(stdout))
        if p.returncode != 0:
            TestMLPiper.skip_cleanup = True
            print("stderr: {}".format(stderr))
            assert p.returncode == 0, err_msg

        return (stdout, stderr)

    def _exec_deploy_or_run_cmdline(self, cmdline_action):
        for pipeline_filepath in TestMLPiper._next_pipeline():
            self._deployment_dir = mkdtemp(prefix='test_mlpiper_deploy', dir='/tmp')
            os.rmdir(self._deployment_dir)

            comp_dir = os.path.join(os.path.dirname(__file__), PYTHON_COMPONENTS_PATH)

            cmd = "{} {} -r {} -f {} --deployment-dir {} --test-mode".format(TestMLPiper.mlpiper_script, cmdline_action, comp_dir,
                                                                    pipeline_filepath, self._deployment_dir)

            self._exec_shell_cmd(cmd, "Failed in '{}' mlpiper command line! {}".format(cmdline_action, cmd))

    def _exec_run_deployment_cmdline(self):
        cmd = "{} run-deployment --deployment-dir {}".format(TestMLPiper.mlpiper_script, self._deployment_dir)
        self._exec_shell_cmd(cmd, "Failed to run existing pipeline deployment! cmd: {}".format(cmd))

    def test_deploy(self):
        self._exec_deploy_or_run_cmdline("deploy")

    def test_deploy_and_run(self):
        self._exec_deploy_or_run_cmdline("run")

    def test_run_deployment(self):
        self._exec_deploy_or_run_cmdline("deploy")
        self._exec_run_deployment_cmdline()

    def test_deploy_and_run_with_models(self):
        self._deployment_dir = mkdtemp(prefix='test_mlpiper_deploy', dir='/tmp')
        os.rmdir(self._deployment_dir)

        comp_dir = os.path.join(os.path.dirname(__file__), PYTHON_COMPONENTS_PATH)

        fd, input_model = mkstemp(prefix='test_mlpiper_pipeline_input_model_', dir='/tmp')
        os.write(fd, json.dumps("Model ZZZ!").encode())
        os.close(fd)

        _, output_model = mkstemp(prefix='test_mlpiper_pipeline_output_model_', dir='/tmp')
        if os.path.exists(output_model):
            os.remove(output_model)

        fd, pipeline_file = mkstemp(prefix='test_mlpiper_pipeline_', dir='/tmp')
        os.write(fd, json.dumps(model_src_sink_pipeline).encode())
        os.close(fd)

        cmd = "{} run -r {} -f {} --input-model '{}' --output-model '{}' --deployment-dir {}" \
            .format(TestMLPiper.mlpiper_script, comp_dir, pipeline_file, input_model, output_model,
                    self._deployment_dir)
        try:
            self._exec_shell_cmd(cmd, "Failed in running pipeline with input/output models!")
        finally:
            os.remove(pipeline_file)
            os.remove(output_model)
            os.remove(input_model)

    def test_run_show_deps(self):
        cmdline_action = "deps"
        comp_dir = os.path.join(os.path.dirname(__file__), PYTHON_COMPONENTS_PATH)

        fd, pipeline_file = mkstemp(prefix='test_deps_pipeline_', dir='/tmp')
        os.write(fd, json.dumps(deps_show_pipeline).encode())
        os.close(fd)

        cmd = "{} {} -r {} -f {} Python".format(TestMLPiper.mlpiper_script, cmdline_action, comp_dir,
                                                pipeline_file)
        try:
            stdout, stderr = self._exec_shell_cmd(cmd, "Failed in '{}' mlpiper command line! {}".format(cmdline_action, cmd))
            l_deps = stdout.decode().split("\n")[1:]
            l_deps = [l for l in l_deps if len(l) > 0]
            assert (len(l_deps) == 4)
            assert ("dep1" in l_deps)
            assert ("dep2" in l_deps)
            assert ("dep345" in l_deps)
            assert ("dep456" in l_deps)
        finally:
            os.remove(pipeline_file)


    def test_parse_component_with_unicode_symbol(self):
        cmdline_action = "deps"
        comp_dir = os.path.join(os.path.dirname(__file__), PYTHON_COMPONENTS_PATH)

        fd, pipeline_file = mkstemp(prefix='test_component_with_unicode_', dir='/tmp')
        os.write(fd, json.dumps(parse_pipeline_with_unicode_symbol_in_component).encode())
        os.close(fd)

        cmd = "{} {} -r {} -f {} Python".format(TestMLPiper.mlpiper_script, cmdline_action, comp_dir,
                                                pipeline_file)
        try:
            stdout, stderr = self._exec_shell_cmd(cmd, "Failed in '{}' mlpiper command line! {}".format(cmdline_action, cmd))
        finally:
            os.remove(pipeline_file)

    def test_run_connected_java_pipeline_with_mlcomp_jar(self):
        self._deployment_dir = mkdtemp(prefix='test_mlpiper_deploy', dir='/tmp')
        os.rmdir(self._deployment_dir)

        comp_dir = os.path.join(os.path.dirname(__file__), JAVA_COMPONENTS_PATH, "test-java-connected-multiple-jars", "target")

        fd, pipeline_file = mkstemp(prefix='test_mlpiper_pipeline_', dir='/tmp')
        os.write(fd, json.dumps(java_connected_pipeline).encode())
        os.close(fd)

        cmd = "{} run -r {} -f {} --deployment-dir {} --mlcomp-jar {} --test-mode" \
            .format(TestMLPiper.mlpiper_script, comp_dir, pipeline_file, self._deployment_dir, self.mlcomp_jar)
        try:
            self._exec_shell_cmd(cmd, "Failed in running pipeline with input/output models!")
        finally:
            os.remove(pipeline_file)
