import argparse
from gridappsd import GridAPPSD, topics
import json
import os

parser = argparse.ArgumentParser()
parser.add_argument("input", help="File to be read one line at a time and sent across the gridappsd message bus")
parser.add_argument("simulation_id", help="Part of the simulation topic to be used for where to send the messages")

options = parser.parse_args()

player_file = options.input
#player_file = "tests/sim_output_13_node.json"
sim_dd = options.simulation_id
#sim_dd = 1882446785
out_topic = topics.simulation_output_topic(sim_dd)

gapps = GridAPPSD()

with open(player_file) as fp:
    do_all = False
    for line in fp:
        message = json.loads(line)
        gapps.send(out_topic, message)

        if not do_all:
            a = input("Press enter to move to next timestep. 'A' for all")
            if a.lower() == 'a':
                do_all = True

  
