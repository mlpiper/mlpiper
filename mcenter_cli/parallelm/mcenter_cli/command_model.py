from cliff.command import Command


class ModelUploadCommand(Command):
    def get_parser(self, prog_name):
        parser = super(ModelUploadCommand, self).get_parser(prog_name)
        parser.add_argument('file', action='store', help='Model file to upload')
        parser.add_argument('name', action='store', help="Model name to use")
        parser.add_argument('format', action='store', help="Model format to use")
        return parser

    def take_action(self, parsed_args):
        self.app.stdout.write("Uploading model: {} {} {}".format(parsed_args.file,
                                                                 parsed_args.name,
                                                                 parsed_args.format))
        desc = "Uploaded by mcenter-cli"
        self.app.mclient.upload_model(parsed_args.name, parsed_args.file, parsed_args.format, description=desc)
