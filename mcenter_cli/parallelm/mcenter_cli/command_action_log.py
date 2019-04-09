from cliff.command import Command

from parallelm.mcenter_cli.list_actions import list_actions


class ActionLogListCommand(Command):
    def get_parser(self, prog_name):
        parser = super(ActionLogListCommand, self).get_parser(prog_name)
        parser.add_argument('--object', action='store', help='List of Objects for which to get actions')
        parser.add_argument('--action', action='store', help='List of actions to query')
        parser.add_argument('--username', action='store', help='List of User, whose actions should be queried')
        parser.add_argument('--status', choices=['both', 'success', 'failed'], help='Show actions with particular staus')
        return parser

    def take_action(self, parsed_args):
        list_actions(self.app.mclient, parsed_args)
