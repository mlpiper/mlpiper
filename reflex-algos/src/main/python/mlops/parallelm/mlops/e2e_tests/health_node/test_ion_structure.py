

from __future__ import print_function

from parallelm.mlops import mlops as mlops


def test_ion_structure():

    print("MyION = {}".format(mlops.get_mlapp_id()))
    print("Nodes:")
    for n in mlops.get_nodes():
        print(n)
    print("\n\n")
    # Getting the ION component object

    curr_node = mlops.get_current_node()
    curr_node_2 = mlops.get_node(curr_node.name)
    if curr_node_2 is None:
        raise Exception("Expecting {} node to exist in this ION".format(curr_node.name))
    print("C0:\n{}".format(curr_node))

    assert curr_node.name == curr_node_2.name

    # Getting the first agent of ion component "0"
    print("Getting node {} agents type of arg:{}".format(curr_node.name, type(curr_node.name)))

    agent_list = mlops.get_agents(curr_node.name)

    if len(agent_list) == 0:
        print("Error - must have agents this ion node is running on")
        raise Exception("Agent list is empty")

    for agent in agent_list:
        if agent is None:
            raise Exception("Agent object obtained is None")
        print("Agent: {}".format(agent))
