import pytest
import tempfile
import os
import shutil
import tarfile

from time_capture_test_helper import TimeCapture1
from parallelm.mlops import mlops as pm
from parallelm.mlops.time_capture.plot_functions import PlotFunctions
from parallelm.mlops.mlops_exception import MLOpsException


def test_time_capture():
    file_name = "tests/timeline_capture_03-ion-kmean-health-alert.tar"
    tc_folder ="timeline-capture-export"
    # TAR time capture directory into a time capture tar file
    try:
        os.remove(file_name)
    except Exception as e:
        pass
    with tarfile.open(file_name, 'w:gz') as tar_obj:
        tar_obj.add("tests/" + tc_folder, tc_folder)

    # Test Init
    pm.init(mlops_mode="stand_alone")
    tc = pm.load_time_capture(input_file=(file_name))
    os.remove(file_name)

    # Test attribute name list
    attribute_names_list = tc.get_attribute_names_list()
    assert (set(attribute_names_list) == set(TimeCapture1.attribute_names_list1))

    # Test get stats
    name1 = "pipelinestat.count"
    df_name = tc.get_stats(name=name1, mlapp_node=None, agent=None, start_time=None,
                           end_time=None)
    assert (list(df_name['values']) == TimeCapture1.count)

    # Test mlspp id
    assert(tc.get_mlapp_id() == TimeCapture1.mlapp_id)

    # Test mlapp policy
    assert(tc.get_mlapp_policy() == TimeCapture1.mlapp_policy)

    # Test get nodes
    assert(tc.get_nodes() == TimeCapture1.mlapp_nodes)

    # Test events
    assert(list(tc.get_events()["eventType"])[0:15] == TimeCapture1.events)

    # Test print matrix
    plot_helper = PlotFunctions(tc)
    plot_helper.print_matrix()
    plot_helper.bar_plot(name1)
    plot_helper.line_plot(name1)

    # Test file saving and get file names
    tempdir = tempfile.mkdtemp()
    tc.save_to_output(tempdir)
    file_names = os.listdir(tempdir)
    assert(set(file_names) == set(TimeCapture1.file_names))
    assert(set(tc.get_file_names()) == set(TimeCapture1.original_file_names))
    shutil.rmtree(tempdir)

    # Test bin rounding
    df_name = tc.get_stats(name="distance", mlapp_node=None, agent=None, start_time=None,
                           end_time=None)
    bins, bins_vert = plot_helper.hist_bin_adapt(df_name)
    assert(bins == TimeCapture1.bins)
    assert(bins_vert == TimeCapture1.bins_vert)

    pm.done()

