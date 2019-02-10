import glob
import json
import os
import pprint
from tempfile import mktemp, mkdtemp
import shutil
import subprocess
import sys


pipeline = {
    "name": "Simple MCenter runner test",
    "engineType": "Python",
    "systemConfig": {
        "statsDBHost": "localhost",
        "statsDBPort": 8086,
        "mlObjectSocketSinkPort": 7777,
        "mlObjectSocketSourcePort": 1,
        "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
        "statsMeasurementID": "1",
        "modelFileSinkPath": "PLACEHOLDER"
    },
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
                "expected-value": "Hello World: testing string source and sink"
            }
        }
    ]
}


class TestMLPiper:
    pipeline_tmp_file = None
    mlpiper_script = None
    egg_paths = []

    @classmethod
    def setup_class(cls):
        TestMLPiper.pipeline_tmp_file = mktemp(prefix='test_mlpiper_pipeline_', dir='/tmp')
        print("pipeline_tmp_file:", TestMLPiper.pipeline_tmp_file)
        with open(TestMLPiper.pipeline_tmp_file, 'w') as f:
            json.dump(pipeline, f)

        mlcomp_root_path = os.path.join(os.path.dirname(__file__), "..")
        os.chdir(mlcomp_root_path)
        subprocess.check_call("make egg", shell=True)

        mlcomp_eggs = glob.glob(os.path.join(mlcomp_root_path, "dist", "*.egg"))
        TestMLPiper.egg_paths.extend(mlcomp_eggs)

        mlops_root_path = os.path.join(os.path.dirname(__file__), "../../mlops")
        os.chdir(mlops_root_path)
        subprocess.check_call("make egg", shell=True)

        mlops_eggs = glob.glob(os.path.join(mlops_root_path, "dist", "*2.*.egg" if sys.version_info[0] < 3 else "*3.*.egg"))
        TestMLPiper.egg_paths.extend(mlops_eggs)

        TestMLPiper.mlpiper_script = os.path.join(mlcomp_root_path, "bin", "mlpiper")
        print("mlpiper_script: {}".format(TestMLPiper.mlpiper_script))

    @classmethod
    def teardown_class(cls):
        if TestMLPiper.pipeline_tmp_file:
            # os.remove(TestMLPiper.pipeline_tmp_file)
            TestMLPiper.pipeline_tmp_file = None

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
        if self._deployment_dir:
            shutil.rmtree(self._deployment_dir)

    def _exec_shell_cmd(self, cmd, err_msg):
        os.environ["PYTHONPATH"] = ":".join(TestMLPiper.egg_paths)

        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(os.environ)

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, env=os.environ)
        (stdout, stderr) = p.communicate()

        if p.returncode != 0:
            print("stdout: {}".format(stdout))
            print("stderr: {}".format(stderr))
            assert p.returncode == 0, err_msg

    def _exec_deploy_or_run_cmdline(self, cmdline_action):
        self._deployment_dir = mkdtemp(prefix='test_mlpiper_deploy', dir='/tmp')
        os.rmdir(self._deployment_dir)

        comp_dir = os.path.join(os.path.dirname(__file__), "../../../../../components/Python")

        cmd = "{} -r {} {} -f {} --deployment-dir {}".format(TestMLPiper.mlpiper_script, comp_dir, cmdline_action,
                                                             TestMLPiper.pipeline_tmp_file, self._deployment_dir)

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
