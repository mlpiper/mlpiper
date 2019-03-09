
import pytest
import unittest

from parallelm.mlops import MLOps
from parallelm.mlops.mlops_mode import MLOpsMode
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats_category import StatCategory, StatGraphType

from parallelm.mlops.channels.mlops_pyspark_channel import MLOpsPySparkChannel
from parallelm.mlops.channels.file_channel import FileChannel, FileChannelOutputFormat
from parallelm.mlops.stats.stat_utils import clean_up, verify_file_content

import protobuf_dep

import pyspark.mllib.common as ml

import pyspark
import json
import os


@pytest.fixture(scope='module')
def prerequisites(request):
    protobuf_dep.copy_protobuf_sources()


class TestMlstatChannel:

    # TODO for Lior: Fix test cases for MATRIX. Disabling these test cases after talking to Lior A.
    """    def test_file_channel_json(self, prerequisites):
        from parallelm.protobuf import InfoType_pb2
        json_test_file = 'good-table-json.json'
        f_handle = open(json_test_file, 'w+')
        expected_output = {"Name": "good-table-json", "Type": InfoType_pb2.General, "Value":
            { "Data": {"1": { "b": 2, "c": 3 }}, "GraphType": "MATRIX", "Mode": "INSTANT", "ShouldTransfer": False,
                           "TransferType": None}}
        fc = FileChannel(file_path=None, file_handle=f_handle, output_fmt=FileChannelOutputFormat.JSON)
        tbl_data = [["a", "b", "c"],
                    [1, 2, 3],
                    [1, 2, 3]]

        fc.stat(json_test_file.split(".")[0], tbl_data, category=StatCategory.CONFIG)

        f_handle.close()

        verify_file_content(json_test_file, expected_output)

        clean_up(json_test_file)

    def test_file_channel_csv(self, prerequisites):

        csv_test_file = 'good-table-csv.csv'
        f_handle = open(csv_test_file, 'w+')
        expected_output = "1515190179.614434, good-table-csv, MATRIX, INSTANT, {'1': {'b': 2, 'c': 3}}"
        pm = FileChannel(file_path=None, file_handle=f_handle, output_fmt=FileChannelOutputFormat.CSV)

        tbl_data = [["a", "b", "c"],
                    [1, 2, 3],
                    [1, 2, 3]]

        pm.stat(csv_test_file.split(".")[0], data=tbl_data, category=StatCategory.CONFIG)
        f_handle.close()

        verify_file_content(csv_test_file, expected_output)

        clean_up(csv_test_file)
    """





