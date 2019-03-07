
import logging


class Group:
    """ ECO Groups handling """

    """ When id is not provided new group will be created on ECO server.
    If group is already created on ECO server,
    id and other data can be provided only to set Group fields. """
    def __init__(self, all_agents_obj, mclient, id=None):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.all_agents_obj = all_agents_obj
        self.group_agents = []
        self.group_agent_objects = []
        self.set_group_agents()
        self._mclient = mclient

        # Create a new group
        if id is None:
            raise Exception("Creating a new group is not supported")
            ids = self.initial_set_group_agent_ids()
            group = self._mclient.create_group(group_name, ids)
            self.id = group['id']
            self._logger.info("Created a new group " + str(group))
        else:
            self.id = id

    def __str__(self):
        return "Group: id: {} agents: [{}]".format(self.id, self.group_agents)

    @property
    def name(self):
        """
        Use ECO rest api
        to query the name
        """
        group = self._mclient.get_group(self.id)
        if group:
            return group['name']

    def set_group_agents(self):
        """
        Set the agent names for the group
        """
        self.group_agent_objects = self.all_agents_obj
        for agent in self.all_agents_obj:
            self.group_agents.append(agent.name)

    def print_self(self):
        self._logger.info(self.name + ':' + str(self.id))

    @property
    def agent_names(self):
        """
        Utility to get agent
        names per workflow
        """
        names = []
        for a in self.group_agent_objects:
            names.append(a.name)
        return names

    @property
    def agents(self):
        """
        :return: Agent[]
        Returns list Agent
        objects
        for the ECO Group
        """
        return self.group_agent_objects

    def initial_set_group_agent_ids(self):
        """
        ONLY for
        initial setup
        of the group data
        """
        ids = []
        for agent in self.all_agents:
            if agent.name in self.group_agents:
                ids.append(agent.id)
        return ids

    def find_group(self, server, name):
        groups = self._mclient.list_groups()
        self._logger.info("Groups on ECO server: " + str(groups))
        for group in groups:
            if group['name'] == name:
                return group
        return None

