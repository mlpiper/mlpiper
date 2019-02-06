from __future__ import print_function

import sys
import argparse
import logging

from parallelm.mlops import mlops as mlops
from parallelm.mlops.e2e_tests.health_node.runner import run_mlops_tests
import parallelm.mlops.e2e_tests.health_node
from parallelm.mlops.constants import Constants


def parse_args():
    print("Got args: {}".format(sys.argv))
    parser = argparse.ArgumentParser()

    parser.add_argument("--test-name", default=None, help="Provide a test to run - if not provided all tests are run")
    options = parser.parse_args()

    return options


def main():
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)

    options = parse_args()

    mlops.init()
    print("MLOps testing")
    print("My {} Node id:   {}".format(Constants.ION_LITERAL, mlops.get_current_node().id))
    print("My {} Node name: {}".format(Constants.ION_LITERAL, mlops.get_current_node().name))
    print("Test name:        {}".format(options.test_name))
    print("Taking test directory from main __file__ value: Name: {} File: {} ".format(__name__, __file__))

    run_mlops_tests(package_to_scan=parallelm.mlops.e2e_tests.health_node, test_to_run=options.test_name)
    mlops.done()


if __name__ == "__main__":
    main()
