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
_log.setLevel(logging.INFO)


def get_opts():
    parser = argparse.ArgumentParser()

    parser.add_argument("simulation_id",
                        help="Simulation id to use for responses on the message bus.")
    parser.add_argument("request",
                        help="GRIDAPPSD based request that is sent from the client to start a simulation.")
    parser.add_argument("log_level",
                        help="The log level for the simulation service overall")

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
    opts.log_level = 'DEBUG'

    if opts.log_level == 'DEBUG':
        opts.log_level = logging.DEBUG
    elif opts.log_level == 'INFO':
        opts.log_level = logging.INFO
    elif opts.log_level == 'ERROR':
        opts.log_level = logging.ERROR
    elif ops.log_level == 'WARNING' or opts.log_level == 'WARN':
        opts.log_level = logging.WARNING
    elif opts.log_level == 'CRITICAL':
        opts.log_level = logging.CRITICAL
    else:
        opts.log_level = 'INFO'

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

    from pprint import pprint
    os.environ['GRIDAPPSD_APPLICATION_ID'] = 'gridappsd-sensor-simulator'
    os.environ['GRIDAPPSD_APPLICATION_STATUS'] = 'STARTED'
    os.environ['GRIDAPPSD_SIMULATION_ID'] = opts.simulation_id
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
    
    #gapp.get_logger().setLevel(opts.log_level)
    read_topic = simulation_output_topic(opts.simulation_id)
    write_topic = service_output_topic(service_id, opts.simulation_id)

    log_file = "/tmp/gridappsd_tmp/{}/sensors.log".format(opts.simulation_id)
    if not os.path.exists(os.path.dirname(log_file)):
        os.makedirs(os.path.dirname(log_file))

    import sys
    from pprint import pprint
    from sensors.measurements import Measurements

    meas = Measurements()
    meta = meas.get_sensors_meta(feeder)
    
    with open(log_file, 'w') as fp:
        logging.basicConfig(stream=fp, level=logging.INFO)
        logging.getLogger().info("Almost ready to create sensors!")
        logging.getLogger().info(f"read topic: {read_topic}\nwrite topic: {write_topic}")
        logging.getLogger().info(f"user options: {user_options}")
        logging.getLogger().debug(f"Meta: {meta}")
        run_sensors = Sensors(gapp, read_topic=read_topic, write_topic=write_topic,
                              user_options=user_options, measurements=meta)
        run_sensors.main_loop()
