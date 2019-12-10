from __future__ import print_function

from pyspark import SparkContext
import sys
import argparse

mlops_loaded = False
try:
    from parallelm.mlops import mlops
    from parallelm.mlops import mlops as pm
    from parallelm.mlops.e2e_tests.health_node.runner import run_mlops_tests
    import parallelm.mlops.e2e_tests.health_node
    from parallelm.mlops.constants import Constants
    mlops_loaded = True
except ImportError:
    pass


def parse_args():
    print("Got args: {}".format(sys.argv))
    parser = argparse.ArgumentParser()

    parser.add_argument("--test-name", default=None, help="Provide a test to run - if not provided all tests are run")
    options = parser.parse_args()

    return options


def main():
    if not mlops_loaded:
        return

    options = parse_args()

    sc = SparkContext(appName="health-test")

    pm.init(sc)
    print("MLOps testing")
    print("My {} Node id:   {}".format(Constants.ION_LITERAL, mlops.get_current_node().id))
    print("My {} Node name: {}".format(Constants.ION_LITERAL, mlops.get_current_node().name))
    print("Test name:        {}".format(options.test_name))

    run_mlops_tests(package_to_scan=parallelm.mlops.e2e_tests.health_node, test_to_run=options.test_name)

    sc.stop()
    pm.done()


if __name__ == "__main__":
    main()
