"""
A small helper for doing stuff with MCenter
"""

# TODO: if mlapp upload fail, delete whatever was created before in the transaction
# TODO: support having variables definition in the mlapp files to be replaced anywhere they appear
# TODO: add make client-tar to create a client tar with all eggs wheels inside
# TODO: add component-run to run a component locally
# TODO: add component-gen to generate a component skelaton.
# TODO: add component-info to show info about a component directory or a component in mcenter
# TODO: add component-list to get a list of components

import argparse
import sys
import logging
import pprint
import json

from parallelm.mcenter_cli import version

from parallelm.mlapp_directory.mlapp_defs import MLAppVersions
from parallelm.mcenter_client.mcenter_client import MCenterClient
from parallelm.mcenter_cli.upload_mlapp import upload_mlapp
from parallelm.mcenter_cli.upload_mlapp_v2 import upload_mlapp_v2
from parallelm.mcenter_cli.delete_mlapp import MLAppDeleteHelper
from parallelm.mcenter_cli.list_mlapp import list_mlapp
from parallelm.mlapp_directory.directory_from_mlapp_builder import DirectoryFromMLAppBuilder
from parallelm.mcenter_cli.upload_component import upload_component
from parallelm.mcenter_cli.execution_environment import ee_download, ee_upload


logo = "<<<<<--------/ mcenter-cli \------->>>>>>"
default_log_level = "error"


class MCenterCli(object):
    def __init__(self, prog_name, logo):
        self._prog_name = prog_name
        self._options = None
        self._mclient = None
        self._logging_level = logging.ERROR
        self._commands = {}
        self._example_text = '''Examples:
            {prog_name} --server localhost mlapp-load /opt/mlapps/regression-prod-v32 
            {prog_name} mlapp-download my-ml-app  /opt/mlapps/my-ml-app-new
            {prog_name} mlapp-delete my-ml-app
            '''.format(prog_name=prog_name)
        self._argv_0 = sys.argv[0]

        self._init_parser(logo)
        self._add_mlapp_commands()
        self._add_ee_commands()
        self._add_component_commands()
        self._add_model_commands()

        self._parse_args()
        self._general_actions()
        self._handle_command()

    def _init_parser(self, logo):
        sys.argv[0] = self._prog_name
        self._parser = argparse.ArgumentParser(description='{0}\n'.format(logo),
                                         formatter_class=argparse.RawDescriptionHelpFormatter,
                                         epilog=self._example_text)
        self._parser.add_argument('--log', default=default_log_level,
                            help='set logging level, default: {}'.format(default_log_level))
        self._parser.add_argument('--verbose', action='store_true', help='set logging level to info')
        self._parser.add_argument('--version', action='store_true', help='show version')

        self._parser.add_argument('-s', '--server', default='localhost', help='address of MCenter server')
        self._parser.add_argument('-u', '--user', default='admin', help='username to use to connect to MCenter')
        self._parser.add_argument("-p", '--password', default='admin', help='password to use for authenticating with MCenter')
        self._subparsers = self._parser.add_subparsers(dest='subparser_name', help='Commands')

    def _register_command(self, command, help, action=None):
        parser = self._subparsers.add_parser(command, help=help)
        self._commands[command] = {"action": action}
        return parser

    def _add_mlapp_commands(self):

        self._register_command('mlapp-list', help='list existing MLApps in MCenter', action=list_mlapp)

        def _mlapp_upload(mclient, args):
            print("Uploading mlapp from {}".format(args.appdir))
            if args.mlapp_version == MLAppVersions.V2:
                upload_mlapp_v2(mclient=mclient, mlapp_dir=args.appdir, force=args.force)
            elif args.mlapp_version == MLAppVersions.V1:
                upload_mlapp(mclient, args.appdir)

        parser = self._register_command('mlapp-upload', help='load MLApp given a directory, use latest format',
                                        action=_mlapp_upload)
        parser.add_argument('appdir', action='store', help='directory to use for loading the MLApp')
        parser.add_argument("--mlapp-version", default=MLAppVersions.V2,
                            help='provide the version of the MLApp format to load')
        parser.add_argument('--force', action='store_true',
                             help='force loading of MLApp, if MLApp already exists remove it first')

        def _mlapp_delete(mclient, args):
            print("Deleting MLApp: {}".format(args.name))
            MLAppDeleteHelper(mclient, args.name, False).delete()

        parser = self._register_command('mlapp-delete', help='delete MLApp from MCenter', action=_mlapp_delete)
        parser.add_argument('name', action='store', help='name of MLApp to delete')

        def _mlapp_download(mclient, args):
            print("Downloading mlapp: {} to {}".format(args.name, args.appdir))
            DirectoryFromMLAppBuilder(mclient, args.name, args.appdir).build()

        parser = self._register_command('mlapp-download', help='download MLApp to a directory')
        parser.add_argument('name', action='store', help='name of MLApp to download')
        parser.add_argument('appdir', action='store', help='directory to use for saving the MLApp to')

    def _add_ee_commands(self):

        def _ee_list(mclient, args):
            json = mclient.ee_list()
            if json is None:
                raise Exception("No valid EE definition received from server")

            fmt = "{:<25} {:<25} {:<25}\n"
            str = fmt.format("Name", "Engine", "ResourceManager")

            for ee in json:
                name = ee["name"]
                eng = ee['configs']['engConfig']['type']
                rm = ee['configs']['rmConfig']['type']
                str += fmt.format(name, eng, rm)
            print(str)

        self._register_command('ee-list', help='list existing Execution Environments in MCenter',
                               action=_ee_list)

        parser = self._register_command('ee-upload', help='load EE given a file, use latest format', action=ee_upload)
        parser.add_argument('eefile', action='store', help='File to use for loading the EE')
        parser.add_argument("--name", action="store", default=None,
                            help="Name to give the EE when uploading. Existing name of EE is overwritten")
        parser.add_argument("--agent", action="store", default=None,
                            help="Name of agent to use when uploading the EE. Existing agent name is overwritten")
        parser.add_argument('--force', action='store_true',
                            help='force loading of EE, if EE already exists remove it first')

        ee_delete_parser = self._subparsers.add_parser('ee-delete', help='delete EE from MCenter')
        ee_delete_parser.add_argument('name', action='store', help='name of EE to delete')

        parser = self._register_command('ee-download', help='download EE to a file', action=ee_download)
        parser.add_argument('name', action='store', help='name of EE to download')
        parser.add_argument('eefile', action='store', help='file to use for saving the EE to')

    def _add_component_commands(self):
        def _component_upload(mclient, args):
            print("Uploading a component: {}".format(args.compdir))
            upload_component(mclient, args.compdir)

        parser = self._register_command('component-upload', help='Upload a component to MCenter',
                                        action=_component_upload)
        parser.add_argument('compdir', action='store', help='Component directory to upload')

    def _add_model_commands(self):
        def _model_upload(mclient, args):
            print("Uploading model: {} {} {}".format(args.file, args.name, args.format))
            desc = "Uploaded by mcenter-cli"
            mclient.upload_model(args.name, args.file, args.format, description=desc)

        model_upload_parser = self._subparsers.add_parser('model-upload', help='Upload a model to MCenter')
        model_upload_parser.add_argument('file', action='store', help='Model file to upload')
        model_upload_parser.add_argument('name', action='store', help="Model name to use")
        model_upload_parser.add_argument('format', action='store', help="Model format to use")

    def _parse_args(self):
        if len(sys.argv) == 1:
            self._parser.print_help(sys.stderr)
            sys.exit(1)
        options = self._parser.parse_args()
        sys.argv[0] = self._argv_0

        if options.subparser_name is None:
            print('No command was provided.\n\n')
            self._parser.print_help(sys.stderr)
            sys.exit(1)

        self._options = options

    def _general_actions(self):
        if self._options.version:
            print("MCenter Client: {}".format(version))
            sys.exit(0)

        if self._options.log in ("INFO", "info") or self._options.verbose:
            self._logging_level = logging.INFO
        elif self._options.log in ("DEBUG", "debug"):
            self._logging_level = logging.DEBUG
        elif self._options.log in ("ERROR", "error", "err"):
            self._logging_level = logging.ERROR
        else:
            self._logging_level = logging.WARN

        logging.basicConfig(level=self._logging_level)

        self._mclient = MCenterClient(server=self._options.server,
                                      user=self._options.user,
                                      password=self._options.password)
        res = self._mclient.login()
        if res is None:
            print("Unable to connect to MCenter. Please check server address, user name and password provided")
            sys.exit(1)

    def _handle_command(self):
        command = self._options.subparser_name

        if command not in self._commands:
            raise Exception("Command {} is not supported".format(self._options.subparser_name))

        command_info = self._commands[command]
        command_info["action"](mclient=self._mclient, args=self._options)


def main():

    MCenterCli(prog_name="mcenter-cli", logo=logo)


if __name__ == "__main__":
    main()
