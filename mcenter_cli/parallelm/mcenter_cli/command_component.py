from cliff.command import Command

from parallelm.mcenter_cli.upload_component import upload_component


class ComponentUploadCommand(Command):
    def get_parser(self, prog_name):
        parser = super(ComponentUploadCommand, self).get_parser(prog_name)
        parser.add_argument('compdir', action='store', help='Component directory to upload')
        return parser

    def take_action(self, parsed_args):
        self.app.stdout.write("Uploading a component: {}".format(parsed_args.compdir))
        upload_component(self.app.mclient, parsed_args.compdir)
