import os
import json
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.data_to_json import DataToJson
from parallelm.mlops.stats_category import StatGraphType


def clean_up(fname):
    try:
        os.remove(fname)
    except OSError:
        pass


def verify_file_content(fname, expected_output):
    f_handle = open(fname, 'r')
    lines = f_handle.readlines()
    first_line = lines[0]

    if 'csv' in fname:
        verify_csv_content(fname, first_line, expected_output)
    else:
        verify_json_content(fname, expected_output)


def verify_csv_content(fname, first_line, expected_content):

    first_line = first_line.strip().split(", ")
    expected_content = expected_content.strip().split(", ")

    first_line_minus_json = first_line[1:4]
    expected_content_minus_json = expected_content[1:4]

    for i, (produced_elem, expected_elem) in enumerate(zip(first_line_minus_json, expected_content_minus_json)):
        if produced_elem != expected_elem:
            raise MLOpsException(
                "File {} has unexpected elem: {}. Expected {}"
                    .format(fname, produced_elem, expected_elem))

    first_line_json_str = ', '.join(first_line[4:]).replace("'", "\"")
    expected_content_json_str = ', '.join(expected_content[4:]).replace("'", "\"")
    first_line_json = json.loads(first_line_json_str)
    expected_content_json = json.loads(expected_content_json_str)

    if first_line_json != expected_content_json:
        raise MLOpsException(
            "File {} has unexpected json: {}. Expected {}"
                .format(fname, first_line_json, expected_content_json))


def verify_json_content(fname, expected_content):

    first_line_dict = json.load(open(fname, 'r'))

    for key in expected_content:

        if key not in first_line_dict:
            raise MLOpsException("File {} did not have key {}".format(fname, key))
        elif key == "Value":
            expected_values = expected_content[key]
            first_line_values = first_line_dict[key]
            sub_keys = ["Data", "GraphType", "Mode", "ShouldTransfer", "TransferType"]
            for sub_key in sub_keys:
                if expected_values[sub_key] != first_line_values[sub_key]:
                    raise MLOpsException("{} did not match: {} {}".format(sub_key, first_line_values[sub_key],
                                                                          expected_values[sub_key]))
        elif expected_content[key] != first_line_dict[key]:
            raise MLOpsException("File {} had a value {} for key {} that did not match {}".format(
                fname, first_line_dict[key], key, expected_content[key]
            ))


