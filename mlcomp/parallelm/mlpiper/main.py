#!/usr/bin/env python3

"""
A runner for running components or pipelines defined using the mlcomp API

prepare - given a component directory + pipeline create the necessary component egg/wheel to run the
          pipeline. And provide the command for running this component from this

run - given previous stage - run the pipeline (can call prepare stage)

Examples:

  # Prepare a deployment
  # Deployment dir can be copied to a docker container and run there
  mlpiper deploy -p p1.json -r components -d /tmp/pp

  # Deploy & Run
  # Useful for development debugging
  mlpiper run -p p1.json -r components -d /tmp/pp

  # Run a deployment
  # Usually non interactive called by another script
  mlpiper run-deployment --deploy-dir /tmp/pp --deps --log debug

"""
# TODO: print mlops output in different color
# TODO: add env variable injection to the pipeline - as engine config
# TODO: support copying a model file/dir to deployment dir
# TODO: Fix pipeline file to use copied model
# TODO: Support installing dependencies packages on top of a deployment directory given pipeline
# TODO: Move MCenter to use mlpiper to prepare the pipeline
# TODO: change the MCenter to use mlpiper for running the pipeline (change the deputy)
# TODO: Support java/scala pipelines

import logging
import argparse
import os
import shutil
import sys
import tempfile

from parallelm.common.constants import LOGGER_NAME_PREFIX
from parallelm.mlpiper.mlpiper import MLPiper
from parallelm.mlpiper.wizard_shell import ComponentWizardShell
from parallelm.mlpiper.wizard_flow import WizardFlowStateMachine
from parallelm.pipeline.component_language import ComponentLanguage
from parallelm.mlcomp import version


LOG_LEVELS = {'debug': logging.DEBUG, 'info': logging.INFO, 'warn': logging.WARN, 'error': logging.ERROR}


class CompRootDirCheck(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        comp_root_dir = values
        if not os.path.isdir(comp_root_dir):
            raise argparse.ArgumentTypeError("--comp-root argument value '{}' has to be existing dir".format(comp_root_dir))
        if os.access(comp_root_dir, os.R_OK):
            setattr(namespace, self.dest, comp_root_dir)
        else:
            raise argparse.ArgumentTypeError("--comp-root argument value '{}' is not a readable dir".format(comp_root_dir))


def parse_args():
    parser = argparse.ArgumentParser(description="Run MLPiper pipelines and components")
    subparsers = parser.add_subparsers(dest='subparser_name', help="Commands")

    _add_deploy_sub_parser(subparsers, 'deploy', 'Deploy a pipeline to run')
    _add_deploy_sub_parser(subparsers, 'run', 'Prepare and run pipeline/component')
    _add_run_deployment_sub_parser(subparsers)
    _add_deps_sub_parser(subparsers)
    _add_wizard_sub_parser(subparsers)

    # General arguments
    parser.add_argument('--version', action='version',
                        version='%(prog)s {version}'.format(version=version))

    parser.add_argument('--conf', required=False, default=None,
                        help="Configuration file for MLPiper runner")

    parser.add_argument('--logging-level', required=False, choices=list(LOG_LEVELS.keys()), default="info")

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


def _add_deploy_sub_parser(subparsers, sub_parser_name, sub_parser_help):
    parser_prepare = subparsers.add_parser(sub_parser_name,
                                           help=sub_parser_help)
    action = parser_prepare.add_mutually_exclusive_group(required=True)
    action.add_argument('-p', '--pipeline',
                        help='A json string, which represents a pipeline.')
    action.add_argument('-f', '--file', type=argparse.FileType('r'),
                        help='A json file path, whose content is a pipeline. Or component JSON')

    parser_prepare.add_argument('-r', '--comp-root', default=None, required=True, action=CompRootDirCheck,
                                help='MLPiper components root dir. Recursively detecting components')

    parser_prepare.add_argument('--input-model',
                                help='Input model file path')
    parser_prepare.add_argument('--output-model',
                                help='Output model file path')

    parser_prepare.add_argument('-d', '--deployment-dir', default=None, required=True,
                                help="Deployment directory to use for placing the pipeline artifacts")

    parser_prepare.add_argument('--force', action='store_true',
                                help='Overwrite any previous generated files/directories (.e.g deployed dir)')

    parser_prepare.add_argument('--mlcomp-jar', default=None, help="Path to mlcomp jar")

    parser_prepare.add_argument('--test-mode', default=False, required=False, action="store_true",
                                help="Run pipeline in test mode")


def _add_run_deployment_sub_parser(subparsers):
    parser_run = subparsers.add_parser('run-deployment',
                                       help='Run mlpiper deployment. Note, this is an internal option.')
    parser_run.add_argument('-d', '--deployment-dir', default=None, required=True,
                            help="Directory containing deployed pipeline")

    parser_run.add_argument('--mlcomp-jar', default=None, help="Path to mlcomp jar")

    parser_run.add_argument('--test-mode', default=False, required=False, action="store_true",
                            help="Run pipeline in test mode")


def _add_deps_sub_parser(subparsers):
    # Get Python/R modules dependencies for the given pipeline or component
    deps = subparsers.add_parser('deps',
                                 help='Show a list of module dependencies for a given pipeline, depending on '
                                      'the components programming language')
    deps.add_argument('lang', choices=[ComponentLanguage.PYTHON, ComponentLanguage.R],
                      help='The programming language')
    group = deps.add_mutually_exclusive_group(required=True)
    group.add_argument('-p', '--pipeline',
                       help='A json string, which represents a pipeline.')
    group.add_argument('-f', '--file', type=argparse.FileType('r'),
                       help='A json file path, whose content is a pipeline. Or component JSON')

    deps.add_argument('-r', '--comp-root', default=None, required=True, action=CompRootDirCheck,
                                help='MLPiper components root dir. Recursively detecting components')


def _add_wizard_sub_parser(subparsers):
    # Get Python/R modules dependencies for the given pipeline or component
    wizard_parser = subparsers.add_parser('wizard', help='Start component creation wizard')
    wizard_parser.add_argument('--editor', action='store_true',
                                help='Start wizard in editor mode')


def main(bin_dir=None):
    options = parse_args()
    if not options:
        return

    logging.basicConfig(format='%(asctime)-15s %(levelname)s %(name)s:  %(message)s')
    logging.getLogger(LOGGER_NAME_PREFIX).setLevel(options.logging_level)

    if options.subparser_name in ("deploy", "run"):
        logging.debug("component_root: {}".format(options.comp_root))

        ml_piper = MLPiper(options) \
            .comp_repo(options.comp_root) \
            .deployment_dir(options.deployment_dir) \
            .mlcomp_jar(options.mlcomp_jar) \
            .bin_dir(bin_dir) \
            .pipeline(options.pipeline if options.pipeline else options.file) \
            .use_color(not options.no_color) \
            .skip_mlpiper_deps_install(options.skip_mlpiper_deps) \
            .force(options.force) \
            .test_mode(options.test_mode)

        if options.input_model:
            ml_piper.input_model(options.input_model)

        if options.output_model:
            ml_piper.output_model(options.output_model)

        ml_piper.deploy()

        if options.subparser_name == "run":
            ml_piper.run_deployment()

    elif options.subparser_name in ("run-deployment"):
        ml_piper = MLPiper(options) \
            .deployment_dir(options.deployment_dir) \
            .skip_mlpiper_deps_install(True) \
            .mlcomp_jar(options.mlcomp_jar) \
            .test_mode(options.test_mode)
        ml_piper.run_deployment()

    elif options.subparser_name in ("deps"):
        tmp_dir = tempfile.mkdtemp()
        ml_piper = MLPiper(options) \
            .comp_repo(options.comp_root) \
            .deployment_dir(tmp_dir) \
            .bin_dir(bin_dir) \
            .pipeline(options.pipeline if options.pipeline else options.file) \
            .use_color(not options.no_color) \
            .force(True)

        ml_piper.deps(options.lang)
        shutil.rmtree(tmp_dir)

    elif options.subparser_name in ("wizard"):
        shell = ComponentWizardShell(shell_name="mlpiper", wizard_edit_mode=options.editor)
        if options.editor:
            rc = shell.cmdloop()
            sys.exit(rc)
        else:
            shell.set_readline_completer()
            sm = WizardFlowStateMachine(shell=shell)
            sm.run()

    else:
        raise Exception("subcommand: {} is not supported".format(options.subparser_name))


if __name__ == '__main__':
    main()
