from cliff.command import Command


class AgentRegisterCommand(Command):
    def get_parser(self, prog_name):
        parser = super(AgentRegisterCommand, self).get_parser(prog_name)
        parser.add_argument('agent', action='store', help='Agent address to register')
        return parser

    def take_action(self, parsed_args):
            self.app.mclient.create_agent(parsed_args.agent)


class AgentListCommand(Command):

    def take_action(self, parsed_args):
        agents = self.app.mclient.list_agents()
        fmt = "{:<15} {:<20}\n"

        self.app.stdout.write(fmt.format("Name:", "Address:"))
        for a in agents:
            self.app.stdout.write(fmt.format(a["name"], a["address"]))
