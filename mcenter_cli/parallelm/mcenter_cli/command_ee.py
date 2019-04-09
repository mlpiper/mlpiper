from cliff.command import Command

from parallelm.mcenter_cli.execution_environment import ee_download, ee_upload


class EEListCommand(Command):
    def take_action(self, parsed_args):
        json = self.app.mclient.ee_list()
        if json is None:
            raise Exception("No valid EE definition received from server")
        fmt = "{:<25} {:<25} {:<25}\n"
        s = fmt.format("Name", "Engine", "ResourceManager")

        for ee in json:
            name = ee["name"]
            eng = ee['configs']['engConfig']['type']
            rm = ee['configs']['rmConfig']['type']
            s += fmt.format(name, eng, rm)
        self.app.stdout.write(s)


class EEUploadCommand(Command):
    def get_parser(self, prog_name):
        parser = super(EEUploadCommand, self).get_parser(prog_name)
        parser.add_argument('eefile', action='store', help='File to use for loading the EE')
        parser.add_argument("--name", action="store", default=None,
                            help="Name to give the EE when uploading. Existing name of EE is overwritten")
        parser.add_argument("--agent", action="store", default=None,
                            help="Name of agent to use when uploading the EE. Existing agent name is overwritten")
        parser.add_argument('--force', action='store_true',
                            help='force loading of EE, if EE already exists remove it first')
        return parser

    def take_action(self, parsed_args):
        ee_upload(self.app.mclient, parsed_args)


class EEDownloadCommand(Command):
    def get_parser(self, prog_name):
        parser = super(EEDownloadCommand, self).get_parser(prog_name)
        parser.add_argument('name', action='store', help='name of EE to download')
        parser.add_argument('eefile', action='store', help='file to use for saving the EE to')
        return parser

    def take_action(self, parsed_args):
        ee_download(self.app.mclient, parsed_args)
