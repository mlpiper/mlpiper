#!/usr/bin/env python3

"""
A runner for running components or pipelines defined using the mlcomp API

prepare - given a component directory + pipeline create the necessary component egg/wheel to run the
          pipeline. And provide the command for running this componeent from this

run - given previous stage - run the pipeline (can call prepare stage)

"""


# TODO: print mlops output in different color
# TODO: add env variable injection to the pipeline - as engine config
# TODO: support copying a model file/dir to deployment dir
# TODO: Fix pipeline file to use copied model
# TODO: Support installing dependencies packages on top of a deployment directory given pipeline
# TODO: Move MCenter to use mlpiper to prepare the pipeline
# TODO: change the MCenter to use mlpiper for running the pipeline (change the deputy)
# TODO: Support java/scala pipelines


import io
import logging
import argparse
import glob
import json
import os
import shutil
import subprocess
import sys

from parallelm.mlpiper.component_scanner import ComponentScanner
from parallelm.pipeline import json_fields
from parallelm.common.base import Base
from parallelm.pipeline.component_language import ComponentLanguage
from parallelm.pipeline.executor import Executor


LOG_LEVELS = {'debug': logging.DEBUG, 'info': logging.INFO, 'warn': logging.WARN, 'error': logging.ERROR}

"""
Examples:

  # Prepare a deployment
  # Deployment dir can be copied to a docker container and run there
  mlpiper deploy -p p1.json -r components -d /tmp/pp 
  
  # Deploy & Run 
  # Usefull for development debugging
  mlpiper run -p p1.json -r components -d /tmp/pp 
  
  # Run a deployment
  # Usually non interactive called by another script
  mlpiper run-deployment --deploy-dir /tmp/pp --deps --log debug
  
  
"""


def parse_args():
    parser = argparse.ArgumentParser(description="Run MLPiper pipelines or components")
    subparsers = parser.add_subparsers(dest='subparser_name', help="Commands")

    # Run pipeline/component
    parser_prepare = subparsers.add_parser('deploy',
                                       help='Deploy a pipeline to run')
    action = parser_prepare.add_mutually_exclusive_group(required=True)
    action.add_argument('-p', '--pipeline',
                        help='A json string, which represents a pipeline.')
    action.add_argument('-f', '--file', type=argparse.FileType('r'),
                        help='A json file path, whose content is a pipeline. Or component JSON')

    parser_prepare.add_argument('--deployment-dir', default='/tmp',
                                help="Deployment directory to use for placing the pipeline artifacts")

    # Prepare and Run pipeline/component
    parser_run = subparsers.add_parser('run',
                                        help='Prepare and run pipeline/component')
    action = parser_run.add_mutually_exclusive_group(required=True)
    action.add_argument('-p', '--pipeline',
                        help='A json string, which represents a pipeline.')
    action.add_argument('-f', '--file', type=argparse.FileType('r'),
                        help='A json file path, whose content is a pipeline. Or component JSON')

    parser_run.add_argument('--deployment-dir', default='/tmp',
                            help="Deployment directory to use for placing the pipeline artifacts")

    # Run deployment
    parser_run = subparsers.add_parser('run-deployment',
                                       help='Run mlpiper deployment. Note, this is an internal option.')
    parser_run.add_argument('--deployment-dir', default='/tmp',
                            help="Deployment directory to use for placing the pipeline artifacts")

    # Get Python/R modules dependencies for the given pipeline or component
    deps = subparsers.add_parser('deps',
                                 help='Return a list of module dependencies for a given pipeline, depending on '
                                      'the components programming language')
    deps.add_argument('lang', choices=[ComponentLanguage.PYTHON, ComponentLanguage.R],
                      help='The programming language')
    group = deps.add_mutually_exclusive_group(required=True)
    group.add_argument('-p', '--pipeline',
                       help='A json string, which represents a pipeline.')
    group.add_argument('-f', '--file', type=argparse.FileType('r'),
                       help='A json file path, whose content is a pipeline. Or component JSON')

    # General arguments
    parser.add_argument('--conf', required=False, default=None,
                        help="Configuration file for MLPiper runner")

    parser.add_argument('--logging-level', required=False, choices=list(LOG_LEVELS.keys()), default="info")

    parser.add_argument('-r', '--comp-root', default=None,
                        help='MLPiper components root dir. Recursively detecting components')

    parser.add_argument('--skip-clean', action="store_true", default=False,
                        help="Do not cleanup deployment after run - useful for debugging pipelines/components")

    parser.add_argument('--no-color', action="store_true", default=False,
                        help="Do not use colors in printouts")

    parser.add_argument('--skip-mlpiper-deps', action="store_true", default=False,
                        help="Skip mlpiper deps install")

    # Spark related arguments
    parser.add_argument('--spark-run-locally', required=False, action='store_true',
                        help='Run Spark locally with as many worker threads as logical cores on your machine.')
    parser.add_argument('--local-cluster', action="store_true",
                        help='Specify whether to run test on local Spark cluster [default: embedded]')

    options = parser.parse_args()
    if not options.subparser_name:
        parser.print_help(sys.stderr)
        return None

    options.logging_level = LOG_LEVELS[options.logging_level]
    return options


def set_logging_level(args):
    if args.logging_level:
        print(args.logging_level)
        logging.getLogger('parallelm').setLevel(LOG_LEVELS[args.logging_level])


class MLPiper(Base):
    SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
    COMPONENT_PACKAGE = "parallelm/code_components"
    COMP_PKG_DIR = "comp_pkg"
    DIST_DIR = "mlpiper_dist"
    COMPONENTS_SETUP_PY = "mcenter_components_setup.py"

    DEPLOYMENT_DEPS_INATALLER = "deployment-deps-installer.sh"
    MLPIPER_SCRIPT = "mlpiper"
    DEPLOYMENT_PIPELINE = "pipeline.json"

    def __init__(self, options):
        super(MLPiper, self).__init__()
        self.set_logger(logging.getLogger(self.logger_name()))
        self._comp_repo_info = None
        self._dist_dir = None
        self._deploy_dir = None
        self._bin_dir = None
        self._pipeline_dict = None
        self._use_color = True
        self._engine = None
        self._options = options

        self._skip_clean = False
        self._skip_mlpiper_deps = False

    def _print_info(self):
        self._logger.info("dist_dir: {}".format(self._dist_dir))
        self._logger.info("bin_dir: {}".format(self._bin_dir))

    def comp_repo(self, comp_repo_info):
        self._comp_repo_info = comp_repo_info
        return self

    def deployment_dir(self, deploy_dir):
        self._deploy_dir = os.path.abspath(deploy_dir)
        return self

    def bin_dir(self, bin_dir):
        self._bin_dir = os.path.realpath(bin_dir)
        return self

    def skip_clean(self, skip_clean):
        self._skip_clean = skip_clean
        return self

    def skip_mlpiper_deps_install(self, skip_deps):
        self._skip_mlpiper_deps = skip_deps
        return self

    def use_color(self, use_color):
        self._use_color = use_color
        return self

    def pipeline(self, pipeline):
        if isinstance(pipeline, io.TextIOWrapper) or isinstance(pipeline, file):
            self._pipeline_dict = json.load(pipeline)
        elif os.path.exists(pipeline):
            self._logger.debug("Detected pipeline as a file")

            with open(pipeline, 'r') as f:
                self._pipeline_dict = json.load(f)
        else:
            self._logger.debug("Trying to load pipeline as a string")
            self._pipeline_dict = json.loads(pipeline)

        self._engine = self._pipeline_dict[json_fields.PIPELINE_ENGINE_TYPE_FIELD]

        return self

    def deploy(self):
        self._logger.info("Preparing pipeline for run")
        self._print_info()

        if not self._deploy_dir:
            raise Exception("Deployment dir was not provided")

        if os.path.exists(self._deploy_dir):
            raise Exception("Deployment dir already exists: {}".format(self._deploy_dir))

        if not self._pipeline_dict:
            raise Exception("Pipeline was not provided")

        if not self._dist_dir:
            self._dist_dir = os.path.join(self._deploy_dir, MLPiper.DIST_DIR)

        self._validate_pipeline()
        self._fix_pipeline()
        self._create_components_pkg()
        self._create_deployment()
        if not self._skip_clean:
            self._cleanup()

    def _get_comp_src_dir(self, comp_name):
        return self._comp_repo_info[self._engine][comp_name]

    def _get_pipeline_components(self):
        return set([e['type'] for e in self._pipeline_dict['pipe']])

    def _validate_pipeline(self):
        if self._engine not in self._comp_repo_info:
            raise Exception("Pipeline Engine: {} has not component".format(self._engine))

        for comp_name in self._get_pipeline_components():
            try:
                self._get_comp_src_dir(comp_name)
            except Exception as e:
                raise Exception("Component {} is not found in loaded components".format(comp_name))

    def _fix_pipeline(self):
        if json_fields.PIPELINE_SYSTEM_CONFIG_FIELD not in self._pipeline_dict:
            self._logger.debug("Adding {} to pipeline".format(json_fields.PIPELINE_SYSTEM_CONFIG_FIELD))
            system_config = {}
            self._pipeline_dict[json_fields.PIPELINE_SYSTEM_CONFIG_FIELD] = system_config

    def _copy_components(self, dest_dir):
        self._logger.debug("Copying pipeline components to staging area")
        comp_copied = {}
        for comp_name in self._get_pipeline_components():
            # Copying each component only once (some pipelines can contain the same component multiple times)
            if comp_name not in comp_copied:
                comp_src_dir = self._get_comp_src_dir(comp_name)
                self._logger.debug("Copying component {} to dst_comp_tmp_dir from {}".format(comp_name, comp_src_dir))
                comp_dst_dir = os.path.join(dest_dir, comp_name)
                shutil.copytree(comp_src_dir, comp_dst_dir)
                comp_copied[comp_name] = True

    def _add_pkg_files(self, comp_pkg_dir):
        self._logger.debug("Copying setup.py to component package")
        src_comp_pkg_setup = os.path.join(self._bin_dir, MLPiper.COMPONENTS_SETUP_PY)
        dst_comp_pkg_setup = os.path.join(comp_pkg_dir, "setup.py")
        shutil.copy(src_comp_pkg_setup, dst_comp_pkg_setup)

        dst_comp_tmp_dir = os.path.join(comp_pkg_dir, MLPiper.COMPONENT_PACKAGE)
        # This is the __init__ at the namespace directory
        with open(os.path.join(dst_comp_tmp_dir, '..', '__init__.py'), 'w') as f:
            f.write("__import__('pkg_resources').declare_namespace(__name__)")

        # This is the __init__ at the lower level dirctory (the directory containing all components as dirs)
        open(os.path.join(dst_comp_tmp_dir, '__init__.py'), 'w').close()

    def _create_pkg(self, comp_pkg_dir):
        create_egg_script = os.path.join(self._bin_dir, "create-egg.sh")
        if not os.path.exists(create_egg_script):
            raise Exception("Script {} does not exists, might be bad installation".format(create_egg_script))

        create_pkg_cmd = '{} --root={} --silent'.format(create_egg_script, comp_pkg_dir)
        self._logger.debug("create_pkg_cmd: {}".format(create_pkg_cmd))

        subprocess.check_call(create_pkg_cmd, shell=True)

    def _create_components_pkg(self):

        comp_pkg_dir = os.path.join(self._dist_dir, MLPiper.COMP_PKG_DIR)
        os.makedirs(comp_pkg_dir)
        self._logger.debug("comp_pkg_dir: {}".format(comp_pkg_dir))
        dst_comp_tmp_dir = os.path.join(comp_pkg_dir, MLPiper.COMPONENT_PACKAGE)
        self._logger.info("dest_comp_tmp_dir: {}".format(dst_comp_tmp_dir))
        os.makedirs(dst_comp_tmp_dir)

        self._copy_components(dst_comp_tmp_dir)
        self._add_pkg_files(comp_pkg_dir)
        self._create_pkg(comp_pkg_dir)

    def _copy_pkg(self, comp_pkg_dir):

        for pkg_filepath in glob.glob(comp_pkg_dir + '/' + 'dist/*.egg'):
            self._logger.debug("Copying pkg {}".format(pkg_filepath))
            shutil.copy(pkg_filepath, self._deploy_dir)

        for pkg_filepath in glob.glob(comp_pkg_dir + '/' + 'dist/*.whl'):
            self._logger.debug("Copying pkg {}".format(pkg_filepath))
            shutil.copy(pkg_filepath, self._deploy_dir)

    def _create_deployment(self):

        comp_pkg_dir = os.path.join(self._dist_dir, MLPiper.COMP_PKG_DIR)
        self._copy_pkg(comp_pkg_dir)

        # TODO - add system config to pipeline
        #self._pipeline_json['systemConfig']['modelFileSinkPath'] = self._model_save_path
        pipeline_file = os.path.join(self._deploy_dir, MLPiper.DEPLOYMENT_PIPELINE)
        with open(pipeline_file, 'w') as f:
            json.dump(self._pipeline_dict, f, indent=4)

        src_entry_point = os.path.join(self._bin_dir, MLPiper.DEPLOYMENT_DEPS_INATALLER)
        dst_entry_point = os.path.join(self._deploy_dir, MLPiper.DEPLOYMENT_DEPS_INATALLER)
        shutil.copy2(src_entry_point, dst_entry_point)

        src_mlpiper = os.path.join(self._bin_dir, MLPiper.MLPIPER_SCRIPT)
        shutil.copy2(src_mlpiper, self._deploy_dir)

    def _cleanup(self):
        self._logger.info("Cleaning up ... " + self._deploy_dir)
        shutil.rmtree(self._dist_dir)

    def run(self):
        if not self._skip_mlpiper_deps:
            self._logger.info("Executing deployment dependencies installer ... {}"
                              .format(MLPiper.DEPLOYMENT_DEPS_INATALLER))
            os.chdir(self._deploy_dir)
            cmd = ["./" + MLPiper.DEPLOYMENT_DEPS_INATALLER]
            try:
                subprocess.check_call(cmd)
            except CalledProcessError as ex:
                print(str(ex))
                return

        self.run_deployment()

    def run_deployment(self):
        self._logger.info("Running prepared deployment, {}".format(self._deploy_dir))

        sys.path.insert(0, self._deploy_dir)
        for egg in glob.glob("{}/*.egg".format(self._deploy_dir)):
            sys.path.insert(0, egg)

        from importlib import reload
        import pkg_resources
        reload(pkg_resources)

        pipeline_file = os.path.join(self._deploy_dir, MLPiper.DEPLOYMENT_PIPELINE)
        if not os.path.exists(pipeline_file):
            raise Exception("Pipeline file not exists! path: {}".format(pipeline_file))

        pipeline_runner = Executor().pipeline_file(open(pipeline_file)).use_color(self._use_color)
        pipeline_runner.go()


def deploy(options, bin_dir):
    components = ComponentScanner.scan_dir(options.comp_root)

    ml_piper = MLPiper(options).comp_repo(components).deployment_dir(options.deployment_dir) \
        .bin_dir(bin_dir).pipeline(options.pipeline if options.pipeline else options.file) \
        .skip_clean(options.skip_clean).use_color(not options.no_color) \
        .skip_mlpiper_deps_install(options.skip_mlpiper_deps)
    ml_piper.deploy()
    return ml_piper


def main(bin_dir=None):
    options = parse_args()
    if not options:
        return

    print(options)

    logging.basicConfig(level=options.logging_level)

    if options.subparser_name in ("deploy"):
        logging.debug("component_root: {}".format(options.comp_root))
        deploy(options, bin_dir)
    elif options.subparser_name in ("run"):
        ml_piper = deploy(options, bin_dir)
        ml_piper.run()
    elif options.subparser_name in ("run-deployment"):
        ml_piper = MLPiper(options).deployment_dir(options.deployment_dir).skip_mlpiper_deps_install(True)
        ml_piper.run_deployment()

    else:
        raise Exception("subcommand: {} is not supported".format(options.subparser_name))


if __name__ == '__main__':
    main()
