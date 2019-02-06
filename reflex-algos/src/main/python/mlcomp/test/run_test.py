#!/usr/bin/env python3
import argparse
from os.path import expanduser
import glob
import json
import os
import re
import shutil
import subprocess
import tempfile
from termcolor import cprint, colored


class TestRunner:
    SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

    def __init__(self, options):
        self._options = options

        self._main_egg_filepath = os.path.join(TestRunner.SCRIPT_DIR, '../dist/*.egg')
        self._test_root_path = tempfile.mkdtemp(suffix='_mlcomp_it')
        self._model_save_path = os.path.join(self._test_root_path, "saved-model")

        self._test_pipeline_path = self._get_pipeline_path()
        self._tmp_comps_egg_dir = tempfile.mkdtemp(suffix='_mlcomp_it_comps')
        self._pipeline_json = None

        if os.path.isdir(self._model_save_path):
            shutil.rmtree(self._model_save_path)

        print("\n*** " + colored("{}".format(os.path.basename(self._test_pipeline_path)), "cyan") + " ***\n")
        fmt = "{:<25} {}"
        print(fmt.format("Pipeline path:", self._test_pipeline_path))
        print(fmt.format("Test dir:", self._test_root_path))
        print(fmt.format("Components egg tmp dir:", self._tmp_comps_egg_dir))
        print("")

    def _get_pipeline_path(self):
        if os.path.isabs(self._options.test):
            test_pipeline_path = self._options.test
        else:
            test_pipeline_path = TestRunner.SCRIPT_DIR + "/" + self._options.test

        if not test_pipeline_path.endswith('.json'):
            test_pipeline_path += '.json'

        if not os.path.isfile(test_pipeline_path):
            raise Exception("Pipeline test file not found! path: {}".format(test_pipeline_path))

        return test_pipeline_path

    def go(self):
        try:
            self._create_main_egg()
            self._load_pipeline()
            self._create_components_egg()
            self._prepare_test_dir()
            self._execute_program()
            self._cleanup()
            cprint("\nTest passed successfully!\n", "green")
        except Exception as e:
            colored("Test failed!\n", "red")
            raise e

    def _create_main_egg(self):
        cmd = TestRunner.SCRIPT_DIR + '/../bin/create-egg.sh --silent'
        subprocess.check_call(cmd, shell=True)

    def _create_components_egg(self):
        dst_comp_tmp_dir = self._tmp_comps_egg_dir + '/parallelm/code_components'
        os.makedirs(dst_comp_tmp_dir)

        comp_names = set([e['type'] for e in self._pipeline_json['pipe']])

        for comp_name in comp_names:
            src_comp_dir = self._options.comps_root + '/' + comp_name
            dst_comp_dir = dst_comp_tmp_dir + '/' + comp_name
            shutil.copytree(src_comp_dir, dst_comp_dir)

        shutil.copy(TestRunner.SCRIPT_DIR + '/setup.py', self._tmp_comps_egg_dir)
        open(dst_comp_tmp_dir + '/__init__.py', 'w').close()

        with open(dst_comp_tmp_dir + '/../__init__.py', 'w') as f:
            f.write("__import__('pkg_resources').declare_namespace(__name__)")

        create_egg_cmd = '{}/../bin/create-egg.sh --root={} --silent'.format(TestRunner.SCRIPT_DIR, self._tmp_comps_egg_dir)
        subprocess.check_call(create_egg_cmd, shell=True)

        for egg_filepath in glob.glob(self._tmp_comps_egg_dir + '/' + 'dist/*.egg'):
            shutil.copy(egg_filepath, self._test_root_path)

    def _load_pipeline(self):
        with open(self._test_pipeline_path, 'r') as f:
            content = f.read()

        pipeline_dir = os.path.realpath(os.path.dirname(self._test_pipeline_path))
        revised_content = re.sub(r'\$__pipeline_dir__\$', pipeline_dir, content, flags=re.M)

        self._pipeline_json = json.loads(revised_content)

    def _prepare_test_dir(self):
        self._dst_test_driver_path = self._test_root_path + '/driver.py'
        main_py_path = os.path.join(TestRunner.SCRIPT_DIR, '../__main__.py')
        shutil.copyfile(main_py_path, self._dst_test_driver_path)

        for egg_filepath in glob.glob(self._main_egg_filepath):
            shutil.copy(egg_filepath, self._test_root_path)

        self._pipeline_json['systemConfig']['modelFileSinkPath'] = self._model_save_path

        self._dst_test_pipeline_path = self._test_root_path + '/' + os.path.basename(self._test_pipeline_path)
        with open(self._dst_test_pipeline_path, 'w') as f:
            json.dump(self._pipeline_json, f)

    def _execute_program(self):
        master = 'spark://localhost:7077' if self._options.local_cluster else 'local[*]'
        eggs = ','.join(glob.glob(self._test_root_path + '/*.egg'))

        spark_submit_tool = os.environ['SPARK_HOME'] + "/bin/spark-submit"
        submit_cmd = '{} --master {} --py-files {} {} exec -f {}'.format(spark_submit_tool, master, eggs,
                                                                         self._dst_test_driver_path,
                                                                         self._dst_test_pipeline_path)

        print("--- Start of Engine Output ---")
        with subprocess.Popen(submit_cmd, shell=True, stdout=subprocess.PIPE, bufsize=1, universal_newlines=True) as p:
            for line in p.stdout:
                print(line)
        print("--- End of Engine Output ---")

        if p.returncode != 0:
            print("Test failed!", "red")
            raise subprocess.CalledProcessError(returncode=p.returncode, cmd=p.args)

        subprocess.check_call(TestRunner.SCRIPT_DIR + '/../bin/cleanup.sh', shell=True)

        print("\n-----------------")

    def _cleanup(self):
        print("Cleaning up ... " + self._tmp_comps_egg_dir)
        shutil.rmtree(self._tmp_comps_egg_dir)

        should_clean = 'y'
        if self._options.ask_clean:
            should_clean = input("\nShould clean up test root path [{}]? [Y|n] "
                                 .format(self._test_root_path)).lower()

        if should_clean == 'y':
            print("Cleaning up ... " + self._test_root_path)
            shutil.rmtree(self._test_root_path)


def parse_args(args):
    parser = argparse.ArgumentParser(description='Run full PySpark integration test')
    parser.add_argument('--test', default='pi-pipeline-rdd.json',
                        help='test pipeline json file path')
    parser.add_argument('--comps-root',
                        default=os.path.join(expanduser("~"), "dev/mlops-components/dev/connectable/pyspark/spark-context"),
                        help='ml components root dir')
    parser.add_argument('--local-cluster', action="store_true",
                        help='Specify whether to run test on local Spark cluster [default: embedded]')
    parser.add_argument('--ask-clean', action="store_true", default=False,
                        help="Wait for user's confirmation before cleanup")
    args = parser.parse_args(args)
    return args


def main(args=None):
    options = parse_args(args)
    TestRunner(options).go()


if __name__ == '__main__':
    main()
