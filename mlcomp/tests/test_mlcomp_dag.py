import json
from tempfile import mkstemp

from parallelm.ml_engine.python_engine import PythonEngine
from parallelm.pipeline.components_desc import ComponentsDesc
from parallelm.pipeline.dag import Dag
from parallelm.pipeline.executor import Executor


class TestPythonIO:

    def test_correct_python_component_io(self):
        pipeline = {
            "name": "stand_alone_test",
            "engineType": "Generic",
            "pipe": [
                {
                    "name": "Test Train 1",
                    "id": 1,
                    "type": "test-python-train",
                    "parents": [],
                    "arguments": {
                        "arg1": "arg1-value"
                    }
                },
                {
                    "name": "Test Train 2",
                    "id": 2,
                    "type": "test-python-train",
                    "parents": [{"parent": 1,
                                 "output": 1,
                                 "input": 1},
                                {"parent": 1,
                                 "output": 0,
                                 "input": 0}
                                ],
                    "arguments": {
                        "arg1": "arg1-value"
                    }
                },
                {
                    "name": "Test Train 3",
                    "id": 3,
                    "type": "test-python-train",
                    "parents": [{"parent": 2,
                                 "output": 0,
                                 "input": 0},
                                {"parent": 2,
                                 "output": 2,
                                 "input": 2},
                                {"parent": 2,
                                 "output": 1,
                                 "input": 1}
                                ],
                    "arguments": {
                        "arg1": "arg1-value"
                    }
                },
                {
                    "name": "Test Train 4",
                    "id": 4,
                    "type": "test-python-train",
                    "parents": [{"parent": 3,
                                 "output": 0,
                                 "input": 1},
                                {"parent": 3,
                                 "output": 1,
                                 "input": 0}
                                ],
                    "arguments": {
                        "arg1": "arg1-value"
                    }
                }
            ]
        }
        python_engine = PythonEngine("test-pipe")
        comps_desc_list = ComponentsDesc(python_engine, pipeline=pipeline).load()
        dag = Dag(pipeline, comps_desc_list, python_engine)

        dag_node_1 = dag.get_dag_node(0)
        dag_node_2 = dag.get_dag_node(1)
        dag_node_3 = dag.get_dag_node(2)
        dag_node_4 = dag.get_dag_node(3)

        # A100 means -- Type A, Node Id 1, Output 0, Goes To 0
        # pipeline is as follow

        #     OUTPUT INDEX 0 - INPUT INDEX 0      OUTPUT INDEX 0 - INPUT INDEX 0      OUTPUT INDEX 0   INPUT INDEX 0
        #    /                              \    /                              \    /              \ /             \
        # ID 1                               ID 2-OUTPUT INDEX 1 - INPUT INDEX 1-ID 3                /\              ID 4
        #    \                              /    \                              /    \              /  \            /
        #     OUTPUT INDEX 1 - INPUT INDEX 1      OUTPUT INDEX 2 - INPUT INDEX 2      OUTPUT INDEX 1    INPUT INDEX 1

        dag.update_parent_data_objs(dag_node_1, ["A100", "B111"])
        dag.update_parent_data_objs(dag_node_2, ["A200", "B211", "C222"])
        dag.update_parent_data_objs(dag_node_3, ["A301", "B310"])

        # as node 1 does not have any parents, input object should be empty
        assert dag.parent_data_objs(dag_node_1) == []
        # as node 2 have input coming but json is not correctly order, but still output should be correctly indexed
        assert dag.parent_data_objs(dag_node_2) == ["A100", "B111"]
        # little complicated node 3 inputs. but same story as above
        assert dag.parent_data_objs(dag_node_3) == ["A200", "B211", "C222"]
        # node 4 gets output of node3's index 0 to its 1st input index and node3's output index 1 to its 0th input indexx
        assert dag.parent_data_objs(dag_node_4) == ["B310", "A301"]
