import os
import logging
import json

from parallelm.mcenter_objects.group import Group
from parallelm.mcenter_objects.agent import Agent
from parallelm.mcenter_objects.workflow import Workflow
from parallelm.mcenter_objects import mcenter_defs

logger = logging.getLogger("upload_mlapp")


def upload_mlapp(mclient, mlapp_dir):
    """
     Uploading MLApp from a directory
     :param mclient: MCenterClient object which can interact with MCenter
     :param mlapp_dir: Directory containing the MLApp files.
     :return: Workflow object representing the MLApp
     """

    if not os.path.isdir(mlapp_dir):
        raise Exception("MLApp path does not exist or is not a directory")

    mlapp_main_file = os.path.join(mlapp_dir, mcenter_defs.MLAPP_MAIN_FILE)
    logger.info("loading ion from: {}".format(mlapp_main_file))
    with open(mlapp_main_file) as owl_json_data:
        mlapp_workflow = json.load(owl_json_data)

    logger.info("Loaded the following MLApp definition from file: {}".format(mlapp_workflow))

    agent_list = mclient.list_agents()
    groups_list = mclient.list_groups()
    logger.info("Agents:  {}".format(agent_list))
    logger.info("Groups:  {}".format(groups_list))

    agents_obj = []
    for agent in agent_list:
        agent_obj = Agent(None, mclient, id=agent["id"])
        logger.info("Agent obj: {}".format(agents_obj))
        agents_obj.append(agent_obj)

    # Groups are already created, so initialize from fetched data
    groups = {}
    for g in groups_list:
        logger.info("Creating group: {}".format(g["name"]))
        group_obj = Group(agents_obj, mclient, id=g["id"])
        logger.info("Group obj: {}".format(group_obj))
        groups[g["name"]] = group_obj

    logger.info("Creating mlapp")
    mlapp = Workflow(mlapp_workflow, mlapp_dir, groups, mclient)
    logger.info("mlapp: {}".format(mlapp))
    return mlapp
