"""
A small helper for doing stuff with MCenter
"""

import argparse
import sys
import logging

from parallelm.mcenter_cli import version

from parallelm.mlapp_directory.mlapp_defs import MLAppVersions
from parallelm.mcenter_client.mcenter_client import MCenterClient
from parallelm.mcenter_cli.upload_mlapp import upload_mlapp
from parallelm.mcenter_cli.upload_mlapp_v2 import upload_mlapp_v2
from parallelm.mcenter_cli.delete_mlapp import MLAppDeleteHelper
from parallelm.mcenter_cli.list_mlapp import list_mlapp
from parallelm.mlapp_directory.directory_from_mlapp_builder import DirectoryFromMLAppBuilder
from parallelm.mcenter_cli.upload_component import upload_component

logo = "<<<<<--------/ mcenter-cli \------->>>>>>"

default_log_level = "error"

# TODO: if mlapp upload fail, delete whatever was created before in the transaction
# TODO: support having variables definition in the mlapp files to be replaced anywhere they appear
# TODO: add make client-tar to create a client tar with all eggs wheels inside
# TODO: add component-run to run a component locally
# TODO: add component-gen to generate a component skelaton.
# TODO: add component-info to show info about a component directory or a component in mcenter
# TODO: add component-list to get a list of components


def parse_args(prog_name, logo):

    example_text = '''Examples:
    {prog_name} --server localhost mlapp-load /opt/mlapps/regression-prod-v32 
    {prog_name} mlapp-download my-ml-app  /opt/mlapps/my-ml-app-new
    {prog_name} mlapp-delete my-ml-app
    '''.format(prog_name=prog_name)

    argv_0 = sys.argv[0]
    sys.argv[0] = prog_name
    parser = argparse.ArgumentParser(description='{0}\n'.format(logo),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=example_text)

    subparsers = parser.add_subparsers(dest='subparser_name', help='Commands')

    mlapp_list_parser = subparsers.add_parser('mlapp-list', help='list existing MLApps in MCenter')

    mlapp_upload_parser = subparsers.add_parser('mlapp-upload', help='load MLApp given a directory, use latest format')
    mlapp_upload_parser.add_argument('appdir', action='store', help='directory to use for loading the MLApp')
    mlapp_upload_parser.add_argument("--mlapp-version", default=MLAppVersions.V2,
                                     help='provide the version of the MLApp format to load')
    mlapp_upload_parser.add_argument('--force', action='store_true',
                                     help='force loading of MLApp, if MLApp already exists remove it first')

    mlapp_delete_parser = subparsers.add_parser('mlapp-delete', help='delete MLApp from MCenter')
    mlapp_delete_parser.add_argument('name', action='store', help='name of MLApp to delete')

    mlapp_download_parser = subparsers.add_parser('mlapp-download', help='download MLApp to a directory')
    mlapp_download_parser.add_argument('name', action='store', help='name of MLApp to download')
    mlapp_download_parser.add_argument('appdir', action='store', help='directory to use for saving the MLApp to')

    component_upload_parser = subparsers.add_parser('component-upload', help='Upload a component to MCenter')
    component_upload_parser.add_argument('compdir', action='store', help='Component directory to upload')

    model_upload_parser = subparsers.add_parser('model-upload', help='Upload a model to MCenter')
    model_upload_parser.add_argument('file', action='store', help='Model file to upload')
    model_upload_parser.add_argument('name', action='store', help="Model name to use")
    model_upload_parser.add_argument('format', action='store', help="Model format to use")


    parser.add_argument('--log', default=default_log_level,
                        help='set logging level, default: {}'.format(default_log_level))
    parser.add_argument('--verbose', action='store_true', help='set logging level to info')
    parser.add_argument('--version', action='store_true', help='show version')

    parser.add_argument('-s', '--server', default='localhost', help='address of MCenter server')
    parser.add_argument('-u', '--user', default='admin', help='username to use to connect to MCenter')
    parser.add_argument("-p", '--password', default='admin', help='password to use for authenticating with MCenter')

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    options = parser.parse_args()
    sys.argv[0] = argv_0

    if options.subparser_name is None:
        print('No command was provided.\n\n')
        parser.print_help(sys.stderr)
        sys.exit(1)

    return options


def main():

    options = parse_args(prog_name="mcenter-cli", logo=logo)

    if options.version:
        print("MCenter Client: {}".format(version))
        sys.exit(0)

    if options.log in ("INFO", "info") or options.verbose:
        logging_level = logging.INFO
    elif options.log in ("DEBUG", "debug"):
        logging_level = logging.DEBUG
    elif options.log in ("ERROR", "error", "err"):
        logging_level = logging.ERROR
    else:
        logging_level = logging.WARN

    logging.basicConfig(level=logging_level)

    mclient = MCenterClient(server=options.server, user=options.user, password=options.password)
    res = mclient.login()
    if res is None:
        print("Unable to connect to MCenter. Please check server address, user name and password provided")
        sys.exit(1)

    if options.subparser_name in ("mlapp-list"):
        list_mlapp(mclient=mclient)
    elif options.subparser_name in ("mlapp-upload"):
        print("Uploading mlapp from {}".format(options.appdir))
        if options.mlapp_version == MLAppVersions.V2:
            upload_mlapp_v2(mclient=mclient, mlapp_dir=options.appdir, force=options.force)
        elif options.mlapp_version == MLAppVersions.V1:
            upload_mlapp(mclient, options.appdir)
    elif options.subparser_name == "mlapp-delete":
        print("Deleting MLApp: {}".format(options.name))
        MLAppDeleteHelper(mclient, options.name, False).delete()
    elif options.subparser_name == "mlapp-download":
        print("Downloading mlapp: {} to {}".format(options.name, options.appdir))
        DirectoryFromMLAppBuilder(mclient, options.name, options.appdir).build()
    elif options.subparser_name == "component-upload":
        print("Uploading a component: {}".format(options.compdir))
        upload_component(mclient, options.compdir)
    elif options.subparser_name == "model-upload":
        print("Uploading model: {} {} {}".format(options.file, options.name, options.format))
        desc = "Uploaded by mcenter-cli"
        mclient.upload_model(options.name, options.file, options.format, description=desc)
    else:
        raise Exception("Command {} is not supported".format(options.subparser_name))


if __name__ == "__main__":
    main()
