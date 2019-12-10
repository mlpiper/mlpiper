from __future__ import print_function

from pyspark import SparkContext

import argparse

mlops_loaded = False
try:
    from parallelm.mlops import mlops as pm
    from parallelm.mlops.mlops_rest_interfaces import MlOpsRestHelper as MLOpsRest
    from parallelm.mlops.data_frame import DataFrameHelper
    from parallelm.mlops.constants import Constants
    mlops_loaded = True
except ImportError:
    pass


def parse_args():

    parser = argparse.ArgumentParser()

    parser.add_argument("--stat_name", help="Name of stat to be retrieved")
    parser.add_argument("--ion_id", help="Id of an {}".format(Constants.ION_LITERAL))
    parser.add_argument("--workflow_id", help="Id of workflow")
    parser.add_argument("--agent_id", help="Id of an agent")
    parser.add_argument("--pipeline_id", help="Id of pipeline")
    parser.add_argument("--start", help="Time from which stats needs to be collected")
    parser.add_argument("--end", help="Time upto which stats needs to be collected")
    parser.add_argument("--output-model", help="Path of output model to create")
    options = parser.parse_args()

    return options


def main():
    if not mlops_loaded:
        return

    options = parse_args()

    sc = SparkContext(appName="mlops-test")

    pm.init(sc)

    # TODO: Add check for arguments
    #if options.output_model is None:
    #    raise Exception("Model output arg is None")
    rest_helper = MLOpsRest("localhost", 3456, "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1NDk2NTA0NzAsInVzZXJuYW1lIjoiYWRtaW4ifQ.3mqBEkppJhSiDyON-guJKTGHSErKUqoqWki2SxY2_vE");

    stat_json = rest_helper.get_stat(options.stat_name, options.ion_id, options.workflow_id, options.agent_id,
                                     options.pipeline_id, options.start, options.end)
    #TODO: Remove folllowing line before checking in
    data_frame_helper = DataFrameHelper()
    print ("Stat DataFrame: {}".format(data_frame_helper.create_frame(stat_json)))

    sc.stop()
    pm.done()

if __name__ == "__main__":
    main()
