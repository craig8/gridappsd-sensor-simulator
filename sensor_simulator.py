from __future__ import absolute_import, print_function

import argparse
import calendar
import csv
import json
import logging
import time
from datetime import datetime

from gridappsd import GridAPPSD, utils
from gridappsd.topics import service_output_topic, simulation_output_topic

from sensors import Sensors

_log = logging.getLogger(__name__)
_log.setLevel(logging.DEBUG)


def get_opts():
    parser = argparse.ArgumentParser()

    parser.add_argument("simulation_id",
                        help="Simulation id to use for responses on the message bus.")
    parser.add_argument("request",
                        help="GRIDAPPSD based request that is sent from the client to start a simulation.")

    # parser.add_argument("--nominal", type=float, default=100.0, nargs='+',
    #                     help="Specify the nominal range of sensor measurements.")
    # parser.add_argument("--perunit-confidence", type=float, default=0.01, nargs='+',
    #                     help="Specify the 95% confidence interval, in +/- perunit of nominal range.")
    # parser.add_argument("--perunit-dropping", type=float, default=0.01, nargs='+',
    #                     help="Fraction of measurements that are not republished.")
    # parser.add_argument("--interval", type=float, default=30.0,
    #                     help="Interval in seconds for min, max, average aggregation.")

    parser.add_argument("-u", "--username", default=utils.get_gridappsd_user(),
                        help="The username to authenticate with the message bus.")
    parser.add_argument("-p", "--password", default=utils.get_gridappsd_pass(),
                        help="The password to authenticate with the message bus.")
    parser.add_argument("-a", "--address", default=utils.get_gridappsd_address(),
                        help="The tcp://addr:port that gridappsd is located on.")
    opts = parser.parse_args()

    assert opts.request, "request must be passed."

    opts.request = json.loads(opts.request)

    return opts


if __name__ == '__main__':
    import os
    import shutil

    sensors = dict()
    opts = get_opts()

    if opts.simulation_id == '-9999':
        raise SystemExit

    if 'test' in opts.request:
        opts.request = {
            "power_system_config": {
                "GeographicalRegion_name": "_73C512BD-7249-4F50-50DA-D93849B89C43",
                "SubGeographicalRegion_name": "_A1170111-942A-6ABD-D325-C64886DC4D7D",
                "Line_name": "_AAE94E4A-2465-6F5E-37B1-3E72183A4E44"
            },
            "application_config": {
                "applications": []
            },
            "simulation_config": {
                "start_time": "1563932301",
                "duration": "120",
                "simulator": "GridLAB-D",
                "timestep_frequency": "1000",
                "timestep_increment": "1000",
                "run_realtime": False,
                "simulation_name": "test9500new",
                "power_flow_solver_method": "NR",
                "model_creation_config": {
                    "load_scaling_factor": "1",
                    "schedule_name": "ieeezipload",
                    "z_fraction": "0",
                    "i_fraction": "1",
                    "p_fraction": "0",
                    "randomize_zipload_fractions": False,
                    "use_houses": False
                }
            },
            "test_config": {
                "events": [],
                "appId": ""
            },
            "service_configs": [{
                "id": "gridappsd-sensor-simulator",
                "user_options": {
                    "default-perunit-confidence-band": 0.02,
                    "sensors-config": {},
                    "default-normal-value": 208,
                    "random-seed": 0,
                    "default-aggregation-interval": 30,
                    "passthrough-if-not-specified": False,
                    "default-perunit-drop-rate": 0.05,
                    "simulate-all": True
                }
            }]
        }
    from pprint import pprint
    pprint(opts.request)
    os.environ['GRIDAPPSD_APPLICATION_ID'] = 'gridappsd-sensor-simulator'
    os.environ['GRIDAPPSD_APPLICATION_STATUS'] = 'STARTED'
    # find the user_options specifically for sensor-simulator
    user_options = None
    for configs in opts.request['service_configs']:
        if configs['id'] == 'gridappsd-sensor-simulator':
            user_options = configs['user_options']
            break
      
    feeder = opts.request['power_system_config']['Line_name']
    service_id = "gridappsd-sensor-simulator"

    gapp = GridAPPSD(username=opts.username,
                     password=opts.password,
                     address=opts.address)

    read_topic = simulation_output_topic(opts.simulation_id)
    write_topic = service_output_topic(service_id, opts.simulation_id)

    log_file = "/tmp/gridappsd_tmp/{}/sensors.log".format(opts.simulation_id)
    if not os.path.exists(os.path.dirname(log_file)):
        os.makedirs(os.path.dirname(log_file))

    import sys
    from pprint import pprint
    from sensors.measurements import Measurements

    meas = Measurements()
    #pprint(meas.get_sensors_config(feeder))
    
    with open(log_file, 'w') as fp:
        logging.basicConfig(stream=fp, level=logging.INFO)
        logging.getLogger().info(f"read topic: {read_topic}\nwrite topic: {write_topic}")
        logging.getLogger().info(f"user options: {user_options}")
        run_sensors = Sensors(gapp, read_topic=read_topic, write_topic=write_topic,
                              user_options=user_options, measurements=meas.get_sensors_meta(feeder))
        run_sensors.main_loop()
