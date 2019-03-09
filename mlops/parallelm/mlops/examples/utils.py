from parallelm.mlops.constants import Constants
from parallelm.mlops import mlops as mlops


class RunModes:
    PYTHON = "python"
    PYSPARK = "pyspark"


def _get_agent_id(node_name, agent_host_name):
    component = mlops.get_node(node_name)
    if component is None:
        raise Exception("Expecting '{}' node to exist in this {}".format(
            node_name, Constants.ION_LITERAL))

    agent_list = mlops.get_agents(node_name)
    if len(agent_list) == 0:
        print("Error - No agents found for node : '{}'".format(node_name))
        raise Exception("Agent list is empty")

    for agent in agent_list:
        print("Agents: {} {}".format(agent.id, agent.hostname))

    try:
        agent = [agent for agent in agent_list if agent.hostname == agent_host_name]
        return agent[0].id
    except Exception as e:
        print("Got exception: No agent found")
        return None

