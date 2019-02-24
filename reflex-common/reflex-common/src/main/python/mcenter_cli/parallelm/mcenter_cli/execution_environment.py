
import sys
import json
import pprint


class EEDefs:
    NAME = "name"
    ID = "id"
    CREATED = "created"
    TEST_PIPELINE_ID = "testPipelineId"
    TEST_MLAPP_ID = "testMLAppId"
    AGENT_ID = "agentId"
    AGENT = "agent"


def del_if_exists(d, field):
    if field in d:
        del(d[field])


def ee_download(mclient, args):
    all_ees = mclient.ee_list()
    ee = list(filter(lambda x: x["name"] == args.name, all_ees))
    if len(ee) > 1:
        raise Exception("More then one EE with the same name!!!!")

    if len(ee) == 0:
        print("Could not find EE: {} ".format(args.name))
        sys.exit(1)

    ee = ee[0]

    # Deleting fields which are not needed when saving as a file
    del_if_exists(ee, EEDefs.ID)
    del_if_exists(ee, EEDefs.TEST_PIPELINE_ID)
    del_if_exists(ee, EEDefs.TEST_MLAPP_ID)

    # Setting the agent ID to agent name
    agent_json = mclient.get_agent(ee[EEDefs.AGENT_ID])
    if agent_json is None:
        raise Exception("Could not get Agent: {}".format(ee[EEDefs.AGENT_ID]))

    del_if_exists(ee, EEDefs.AGENT_ID)
    ee[EEDefs.AGENT] = agent_json["host"]

    f = open(args.eefile, "w")
    json.dump(ee, f, indent=4)
    f.close()


def ee_upload(mclient, args):

    f = open(args.eefile)
    ee = json.load(f)
    if args.name is not None:
        ee[EEDefs.NAME] = args.name

    if args.agent is not None:
        ee[EEDefs.AGENT] = args.agent

    # Getting the agent_id
    agent_name = ee[EEDefs.AGENT]
    agents = mclient.list_agents()
    if agents is None:
        raise Exception("Could not receive agent list")
    if len(agents) == 0:
        raise Exception("No agents are defined in MCenter")

    agent = list(filter(lambda x: x["host"] == agent_name, agents))
    if len(agent) == 0:
        raise Exception("Could not find agent {} in MCenteer".format(agent_name))
    if len(agent) > 1:
        raise Exception("There are more then one agent with name: {}".format(agent_name))

    agent = agent[0]
    del_if_exists(ee, EEDefs.AGENT)
    ee[EEDefs.AGENT_ID] = agent["id"]
    mclient.ee_create(ee)
