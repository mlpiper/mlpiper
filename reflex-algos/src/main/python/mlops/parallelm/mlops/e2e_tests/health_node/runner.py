from __future__ import print_function

import copy
import os
import sys
import traceback
import importlib

from parallelm.mlops import mlops as pm
from parallelm.mlops.stats_category import StatCategory as st
from parallelm.mlops.stats.table import Table
from parallelm.mlops.e2e_tests.e2e_constants import E2EConstants

import os.path, pkgutil


def detect_modules_in_dir(dir, prefix="test_"):
    print("Detecting modules starting with [{}] at dir [{}]".format(prefix, dir))

    modules = []
    files = os.listdir(dir)
    for file in files:
        if file.startswith(prefix) and file.endswith(".py"):
            file = file.strip(".py")
            modules.append(file)
    return modules


def detect_modules_in_package(package, prefix="test_"):
    print("Detecting modules starting with [{}] at package [{}]".format(prefix, package))
    pkg_path = os.path.dirname(package.__file__)
    all_modules = [name for _, name, _ in pkgutil.iter_modules([pkg_path])]

    modules = []

    for module in all_modules:
        if module.startswith(prefix):
            modules.append(module)
    return modules


def detect_module_methods(module, prefix="test_"):
    mod_filtered_funcs = []
    mod_funcs = dir(module)
    for func in mod_funcs:
        if func.startswith(prefix):
            mod_filtered_funcs.append(func)
    return mod_filtered_funcs


def run_module_method_by_name(mod, method):
    method_to_call = getattr(mod, method)


def run_mlops_tests(package_to_scan, test_to_run=None):
    """
    Given a directory, scan the directory and take all files starting with test_ in each file
    run all the functions starting with test_

    TODO: find a way to use pytest here if possible.

    :param package_to_scan: package to scan for test modules
    :param test_to_run: If provided run only a specific test "module.func"
    :raise Exception: In case of error in the tests an Exception will be raised
    """

    modules = detect_modules_in_package(package_to_scan)
    print("Detected modules: {}".format(modules))
    print("Loading and running test_XXX methods inside")

    results = []
    failed_tests = 0
    total_tests = 0

    for mod_name in modules:
        mod = importlib.import_module(package_to_scan.__name__ + "." + mod_name)

        mod_funcs = detect_module_methods(mod)

        module_results = dict()
        module_results["name"] = mod_name
        module_results["per_func"] = []
        module_results["pass"] = True
        print("Module {} funcs {}".format(mod, mod_funcs))

        for func_name in mod_funcs:
            if test_to_run is not None:
                full_test_name = "{}.{}".format(mod_name, func_name)
                if full_test_name != test_to_run:
                    continue

            total_tests += 1
            func_results = dict()
            func_results["name"] = func_name
            print("\n\nrunning test: {}.{}".format(mod_name, func_name))
            try:
                method_to_call = getattr(mod, func_name)
                method_to_call()
                func_results["pass"] = True
            except Exception as e:
                func_results["pass"] = False
                func_results["traceback"] = "".join(traceback.format_exception(*sys.exc_info()))
                failed_tests += 1
                module_results["pass"] = False
            module_results["per_func"].append(func_results)
        results.append(module_results)


    # Table
    tbl = Table().name("Test Results").cols(["Module", "Test", "Status"])

    print("\n\n\n")
    print("Test Summary: total: {} ok: {} failed: {}".format(total_tests, total_tests - failed_tests, failed_tests))
    print("=======================================================")
    idx = 0
    for mod_res in results:
        print("Module: {}".format(mod_res["name"]))
        for func_res in mod_res["per_func"]:

            if test_to_run is not None:
                full_test_name = "{}.{}".format(mod_res["name"], func_res["name"])
                if full_test_name != test_to_run:
                    continue

            print("|    {:<20} {}".format(func_res["name"], func_res["pass"]))
            if func_res["pass"] is False:
                print("\n{}\n".format(func_res["traceback"]))

            tbl.add_row(str(idx), [mod_res["name"], func_res["name"], "pass" if func_res["pass"] else "fail"])
            idx += 1

    pm.set_stat(tbl)
    pm.set_stat(E2EConstants.E2E_RUN_STAT, 1, st.TIME_SERIES)
    if failed_tests > 0:
        print("=======================================================\n")
        print("Aborting unit test due to errors")
        raise Exception("Failed running unit tests! failed: {}\n".format(failed_tests))

