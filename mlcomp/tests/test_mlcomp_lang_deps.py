import json
from tempfile import mkstemp

from parallelm.pipeline.executor import Executor


expected_py_deps = ["scikitlearn==1.3.2", "docopt", "kazoo", "numpy"]
expected_r_deps = ["optparse", "reticulate"]


pipeline = {
    "name": "some-pipeline",
    "engineType": "Generic",
    "systemConfig": {
        "statsDBHost": "localhost",
        "statsDBPort": 8086,
        "mlObjectSocketSinkPort": 7777,
        "mlObjectSocketSourcePort": 1,
        "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
        "statsMeasurementID": "1",
        "modelFileSinkPath": "__model_out_filepath__"
    },
    "pipe": [
        {
            "name": "Comp-1",
            "id": 1,
            "type": "py_deps_comp1",
            "parents": [],
            "arguments": {
                "arg1": "arg1-value"
            }
        },
        {
            "name": "Comp-2",
            "id": 2,
            "type": "py_deps_comp2",
            "parents": [],
            "arguments": {
                "arg1": "arg1-value"
            }
        },
        {
            "name": "Hello World",
            "id": 3,
            "type": "hello-world",
            "parents": [],
            "arguments": {
                "arg1": "arg1-value"
            }
        },
        {
            "name": "Comp-3 (R)",
            "id": 4,
            "type": "r_deps_comp3",
            "parents": [],
            "arguments": {
                "arg1": "arg1-value"
            }
        }
    ]
}


class TestPythonDeps:
    pipeline_tmp_file = None

    @classmethod
    def setup_class(cls):
        fd, TestPythonDeps.pipeline_tmp_file = mkstemp(prefix='test_py_deps_', dir='/tmp')
        print("pipeline_tmp_file:", TestPythonDeps.pipeline_tmp_file)
        with open(TestPythonDeps.pipeline_tmp_file, 'w') as f:
            json.dump(pipeline, f)

    @classmethod
    def teardown_class(cls):
        if TestPythonDeps.pipeline_tmp_file:
            # os.remove(TestPythonDeps.pipeline_tmp_file)
            TestPythonDeps.pipeline_tmp_file = None

    def test_accumulated_python_deps(self):
        with open(TestPythonDeps.pipeline_tmp_file, 'r') as f:
            pipeline_runner = Executor(args=None).pipeline_file(f)
            deps = pipeline_runner.all_py_component_dependencies()
            assert deps == set(expected_py_deps)

    def test_accumulated_r_deps(self):
        with open(TestPythonDeps.pipeline_tmp_file, 'r') as f:
            pipeline_runner = Executor(args=None).pipeline_file(f)
            deps = pipeline_runner.all_r_component_dependencies()
            assert deps == set(expected_r_deps)
