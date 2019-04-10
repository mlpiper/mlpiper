import pprint

from cliff.command import Command

from parallelm.mlapp_directory.mlapp_defs import MLAppVersions
from parallelm.mlapp_directory.mlapp_defs import MLAppProfileKeywords
from parallelm.mcenter_cli.upload_mlapp import upload_mlapp
from parallelm.mcenter_cli.upload_mlapp_v2 import upload_mlapp_v2
from parallelm.mcenter_cli.delete_mlapp import MLAppDeleteHelper
from parallelm.mlapp_directory.directory_from_mlapp_builder import DirectoryFromMLAppBuilder


class MLAppListCommand(Command):
    def take_action(self, parsed_args):
        fmt = "{:<35} {:<35}"
        s = ""
        s += fmt.format("Profile Name", "Pattern Name")
        for profile_info in self.app.mclient.list_ion_profiles():
            profile_name = profile_info[MLAppProfileKeywords.NAME]
            pattern_name = profile_info[MLAppProfileKeywords.PATTERN_NAME]
            if len(s) > 0:
                s += "\n"
            s += fmt.format(profile_name, pattern_name)
        self.app.stdout.write(s)


class MLAppInfoCommand(Command):
    def get_parser(self, prog_name):
        parser = super(MLAppInfoCommand, self).get_parser(prog_name)
        parser.add_argument('name', action='store', help='Name of MLApp to query for info')
        return parser

    def take_action(self, parsed_args):
        all_profiles = self.app.mclient.list_ion_profiles()
        mlapp_info = list(filter(lambda x: x["name"] == parsed_args.name, all_profiles))
        if len(mlapp_info) == 0:
            raise Exception("Could not find mlapp with name: {}".format(parsed_args.name))
        if len(mlapp_info) > 1:
            raise Exception("Found {} mlapps with the same name".format(len(mlapp_info)))
        mlapp_info = mlapp_info[0]
        self.app.stdout(pprint.pformat(mlapp_info))


class MLAppUploadCommand(Command):
    def get_parser(self, prog_name):
        parser = super(MLAppUploadCommand, self).get_parser(prog_name)
        parser.add_argument('name', action='store', help='Name of MLApp to query for info')

        parser.add_argument('appdir', action='store', help='Directory to use for loading the MLApp')
        parser.add_argument("--mlapp-version", default=MLAppVersions.V2,
                            help='Provide the version of the MLApp format to load')
        parser.add_argument('--force', action='store_true',
                            help='Force loading of MLApp, if MLApp already exists remove it first')
        parser.add_argument('--ee', action='store',
                            help='Use the given ee (Execution Environment) when creating the MLApp')

        return parser

    def take_action(self, parsed_args):

        self.app.stdout("Uploading mlapp from {}".format(parsed_args.appdir))
        if parsed_args.mlapp_version == MLAppVersions.V2:
            upload_mlapp_v2(mclient=self.app.mclient,
                            mlapp_dir=parsed_args.appdir,
                            force=parsed_args.force,
                            ee=parsed_args.ee)
        elif parsed_args.mlapp_version == MLAppVersions.V1:
            upload_mlapp(self.app.mclient, parsed_args.appdir)
        else:
            raise Exception("MLApp version {} is not supported".format(parsed_args.mlapp_version))


class MLAppDeleteCommand(Command):
    def get_parser(self, prog_name):
        parser = super(MLAppDeleteCommand, self).get_parser(prog_name)
        parser.add_argument('name', action='store', help='Name of MLApp to delete')
        return parser

    def take_action(self, parsed_args):
        self.app.stdout("Deleting MLApp: {}".format(parsed_args.name))
        MLAppDeleteHelper(self.app.mclient, parsed_args.name, False).delete()


class MLAppDownloadCommand(Command):
    def get_parser(self, prog_name):
        parser = super(MLAppDownloadCommand, self).get_parser(prog_name)
        parser.add_argument('name', action='store', help='Name of MLApp to download')
        parser.add_argument('appdir', action='store', help='Directory to use for saving the MLApp')
        return parser

    def take_action(self, parsed_args):
        self.app.stdout("Downloading mlapp: {} to {}".format(parsed_args.name, parsed_args.appdir))
        DirectoryFromMLAppBuilder(self.app.mclient, parsed_args.name, parsed_args.appdir).build()

