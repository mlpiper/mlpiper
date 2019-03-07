
import logging


class Agent:
    """
    Handles ECO agents.
    """
    def __init__(self, node, mclient, id = None):
        """
        When id is not provided new agent will be created on ECO server.
        If agent is already created on ECO server,
        id and other data can be provided only to set Agent fields.

        :param node: Node name or address to use
        :param mclient: MCenter client object to user for communicating with MCenter
        :param id: ID of agent, if not None use this ID to fetch info from server, otherwise create the agent
        """

        self._logger = logging.getLogger(self.__class__.__name__)
        self._mclient = mclient

        if id is None:
            agent_info = self._mclient.create_agent(node)
            self._logger.info("Created new agent " + str(agent_info))
            self.id = agent_info.get('id')
        else:
            self.id = id

    def __str__(self):
        s = ""
        s += "Agent: name: {} id: {} address: {}".format(self.name, self.id, self.address)
        return s

    @property
    def name(self):
        info = self._mclient.get_agent(self.id)
        return info.get('name')

    @property
    def address(self):
        info = self._mclient.get_agent(self.id)
        return info.get('address')

    @property
    def agent_log_path(self):
        return ''

    def print_self(self):
        self._logger.info(self.name + ': ' + self.id)

    def find_agent(self, check_node):
        """
        Checks if reflex already
        have registered agent instance
        """
        for node in self._mclient.list_agents():
            if node.get('address') == check_node:
                return node
        return None

    def is_engine_alive(self):
        """
        Agent queries the engine
        of its own type and
        checks if it is
        alive through REST api
        call
        """
        return False
