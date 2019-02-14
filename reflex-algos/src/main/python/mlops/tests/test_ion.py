
import pytest

from parallelm.mlops.ion.ion_builder import IONBuilder
from ion_test_helper import ION1, test_workflow_instances


def test_ion_builder():
    ion_builder = IONBuilder()
    ion = ion_builder.build_from_dict(test_workflow_instances)
    assert ion is not None
    assert ion.id == ION1.ION_ID
    ion_nodes = ion.nodes
    assert len(ion_nodes) == 2
    assert ion_nodes[0].id == ION1.NODE_0_ID
    assert ion_nodes[0].pipeline_pattern_id == ION1.PIPELINE_PATTERN_ID_0
    assert ion_nodes[0].pipeline_instance_id == ION1.PIPELINE_INST_ID_0
    assert ion_nodes[0].ee_id == ION1.EE_ID_0
    print("ION: {}".format(ion))
