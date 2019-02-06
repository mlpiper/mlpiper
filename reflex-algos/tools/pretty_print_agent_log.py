import os
import sys
import json
import argparse
import subprocess
import re
def main(options):
    json_pipe_err = 'com.parallelmachines.eco.agent.runners.flink.PMPipelineRunner' 
    agent_log = options.path 
    
    if agent_log == "":
        # based off of: "LOG=ps -aux | grep java| grep ECOAgent | awk '{print $12}' | sed -e 's/-Dlog.file=//g'"
        proc = subprocess.Popen(["ps", "aux"], stdout=subprocess.PIPE,  stderr=subprocess.PIPE)
        output, err = proc.communicate()
        output = str(output)
        if "ECOAgent" not in output:
            print("No path supplied and ECOAgent not running!")
            sys.exit(1)
        lines = output.split('\\n') 
        for line in lines:
            line = line.strip()
            if 'ECOAgent' in line:
                #print(line)
                # should work as long as process param format remains the same
                elems = line.split()
                agent_log = ""
                for elem in elems:
                    if 'log.file' in elem:
                        agent_log = elem.split('=')[1]
                        break
                if agent_log == "":
                    print("Log not found")
                    sys.exit(1)
                else:
                    print("No path supplied. Using {}".format(agent_log))
    elif len(agent_log) < 4 or '.log' != agent_log[-4:]:
        # assumption: if the exact path is not supplied and something is entered as 
        # an arg, then it is assumed this arg is the install path
        old_path = agent_log
        agent_log += '/pm-eco-0.9.0/log/bran-c8-eco-agent.log'
        print("assumed {} is the install directory. Making log path {}".format(old_path, agent_log))
    
    if os.path.isfile(agent_log):

        with open(agent_log, 'r') as infile:
            lines = infile.readlines()
            for line in lines:
                line = line.strip()
                elems = line.split()
                # TODO: check assumptions hold always
                if len(elems) > 6 and elems[2] == "ERROR" and elems[3] == json_pipe_err:
                    json_str = ' '.join(elems[7:])
                    json_obj = json.loads(json_str)
                    print(json_obj["error"])
    else:
        print("Could not resolve path {}".format(agent_log))

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--path", default="")
   
    options = parser.parse_args()

    main(options)
