import sys
import logging

from cliff.app import App
from cliff.commandmanager import CommandManager

from parallelm.mcenter_cli import version
from parallelm.mcenter_client.mcenter_client import MCenterClient


class MCli(App):

    DEFAULT_LOG_LEVEL = "error"

    def __init__(self, prog_name):

        super(MCli, self).__init__(
            description='mcli - MCenter CLI',
            version=version,
            command_manager=CommandManager('mcenter.cli'),
            deferred_help=True,
        )

        self._prog_name = prog_name
        self._options = None
        self.mclient = None
        self._logging_level = logging.ERROR
        self._commands = {}
        self._example_text = '''Examples:
            {prog_name} --server localhost mlapp-load /opt/mlapps/regression-prod-v32 
            {prog_name} mlapp-download my-ml-app  /opt/mlapps/my-ml-app-new
            {prog_name} mlapp-delete my-ml-app
            {prog_name} get-action-log --object=MLApp,Model --action=Stop,Upload --username=admin,MLOPSUser --status=success
            '''.format(prog_name=prog_name)
        self._argv_0 = sys.argv[0]

        # self._init_parser(logo)
        # self._add_mlapp_commands()
        # self._add_ee_commands()
        # self._add_component_commands()
        # self._add_model_commands()
        # self._add_action_log_commands()
        # self._add_agent_commands()
        # self._parse_args()
        # self._general_actions()
        # self._handle_command()

    def _general_actions(self):

        if self.options.log in ("INFO", "info") or self.options.verbose_level:
            self.logging_level = logging.INFO
        elif self.options.log in ("DEBUG", "debug"):
            self.logging_level = logging.DEBUG
        elif self.options.log in ("ERROR", "error", "err"):
            self.logging_level = logging.ERROR
        else:
            self.logging_level = logging.WARN

        logging.basicConfig(level=self._logging_level)

        if not self.options.deferred_help:
            self.mclient = MCenterClient(server=self.options.server,
                                         user=self.options.user,
                                         password=self.options.password)
            res = self.mclient.login()
            if res is None:
                print("Unable to connect to MCenter. Please check server address, user name and password provided")
                sys.exit(1)

    def build_option_parser(self, description, version, argparse_kwargs=None):
        parser = super(MCli, self).build_option_parser(description, version)

        parser.add_argument('--log', default=MCli.DEFAULT_LOG_LEVEL,
                            help='set logging level, default: {}'.format(MCli.DEFAULT_LOG_LEVEL))

        parser.add_argument('-s', '--server', default='localhost', help='address of MCenter server')
        parser.add_argument('-u', '--user', default='admin', help='username to use to connect to MCenter')
        parser.add_argument("-p", '--password', default='admin', help='password to use for authenticating with MCenter')
        return parser

    def initialize_app(self, argv):
        self.LOG.debug('initialize_app')
        self._general_actions()

    def prepare_to_run_command(self, cmd):
        self.LOG.debug('prepare_to_run_command %s', cmd.__class__.__name__)

    def clean_up(self, cmd, result, err):
        self.LOG.debug('clean_up %s', cmd.__class__.__name__)
        if err:
            self.LOG.debug('got an error: %s', err)


def main(argv=sys.argv[1:]):
    myapp = MCli("mcli")
    return myapp.run(argv)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))