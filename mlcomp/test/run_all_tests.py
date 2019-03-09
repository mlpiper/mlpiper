#!/usr/bin/env python3
import argparse
import os
import traceback
import run_test
from termcolor import cprint, colored

if "SPARK_HOME" not in os.environ:
    raise SystemError("'SPARK_HOME' environment variable is not set! "
                      "Please make sure to set it properly!")

parser = argparse.ArgumentParser(description='Run all PySpark integration tests')
parser.add_argument('--local-cluster', action="store_true",
                    help='Specify whether to run all the tests using local Spark cluster [default: embedded]')
parser.add_argument('--verbose', action="store_true",
                    help='Whether to print additional info')
args = parser.parse_args()

script_dir = os.path.realpath(os.path.dirname(__file__))
pipelines_root = os.path.join(script_dir, "pipelines")
test_tool_path = os.path.join(script_dir, "run_test.py")


def next_test():
    with open(os.path.join(pipelines_root, "manifest.txt")) as f:
        for line in f.readlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            test_and_comp_root = line.split(",")
            if len(test_and_comp_root) != 2:
                print("Invalid test entry: {}".format(test_and_comp_root))
                continue

            yield (test_and_comp_root[0].strip(), test_and_comp_root[1].strip())


total_tests = 0
test_passed = 0
for test_info in next_test():
    total_tests += 1
    pipeline_path = os.path.join(pipelines_root, test_info[0])
    comp_root_path = os.path.join(os.path.expanduser(test_info[1]))
    test_args = ["--test", pipeline_path, "--comps-root", comp_root_path]
    if args.local_cluster:
        test_args.append("--local-cluster")
    try:
        if args.verbose:
            print("Args (run_test.py):", colored("{}".format(" ".join(test_args)), "blue", "on_white", attrs=['bold']))
        run_test.main(test_args)
        test_passed += 1
    except:
        traceback.print_exc()

print(30 * "#", "\n")
cprint("{}/{} tests passed successfully!\n".format(test_passed, total_tests), "green" if test_passed == total_tests else "yellow", attrs=['bold'])
