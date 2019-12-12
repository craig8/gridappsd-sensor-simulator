import argparse
import calendar
import csv
import json
import logging
import math
import sys
import time
from datetime import datetime

from gridappsd import GridAPPSD, utils, topics as t
from gridappsd.topics import service_output_topic, simulation_output_topic

from sensors import Sensors
from sensors.measurements import SparqlMeasurements
from sensors.sensordao import SensorDao
from sensors.user_config import UserConfig

_log = logging.getLogger(__name__)


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

    print(opts.request)
    opts.request = json.loads(opts.request)

    return opts


if __name__ == '__main__':
    import os
    import shutil

    opts = get_opts()

    if opts.simulation_id == '-9999':
        raise SystemExit

    # Need to build a class for parsing configs from json.
    user_options = opts.request['service_configs'][0]['user_options']
    feeder = opts.request['power_system_config']['Line_name']
    # Take care of conversion from the simulation to the correct number
    # of seconds by using time_multiple, note this is
    service_id = "gridappsd-sensor-simulator"

    logfile = f"/tmp/gridappsd_tmp/{opts.simulation_id}/sensor-simulator.log"
    if not os.path.exists(os.path.dirname(logfile)):
        os.makedirs(os.path.dirname(logfile))

    logging.basicConfig(level=logging.INFO,
                        stream=sys.stdout,
                        format="%(asctime)s;%(levelname)s;%(message)s")
    #sh = logging.StreamHandler()
    fh = logging.FileHandler(logfile)
    fmter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")
    #sh.setFormatter(fmter)
    fh.setFormatter(fmter)
    #sh.setLevel(logging.INFO)
    fh.setLevel(logging.DEBUG)
    #logging.getLogger().addHandler(sh)
    logging.getLogger().addHandler(fh)

    os.environ['GRIDAPPSD_APPLICATION_STATUS'] = 'RUNNING'
    os.environ["GRIDAPPSD_APPLICATION_ID"] = service_id

    gapp = GridAPPSD(username=opts.username,
                     password=opts.password,
                     address=opts.address)

    logger = gapp.get_logger()
    logger.debug("{service_id} starting with sim id {sim_id}".format(
        service_id=service_id, sim_id=opts.simulation_id))
    read_topic = simulation_output_topic(opts.simulation_id)
    write_topic = service_output_topic(service_id, opts.simulation_id)

    sparql_queries = SparqlMeasurements(gapp, feeder)
    t0 = time.time()
    equipment_nomv = sparql_queries.get_nominal_voltages()
    t1 = time.time()
    energy_measurements = sparql_queries.get_energy_consumer_measurements()
    t2 = time.time()

    print(f"""
        Time to get measurements {t1 - t0}
        Time to get consumers {t2 - t1}
        """)
        #Time to get cim dictionary {t3 - t2}
        #""")

    _log.debug("Setting up for sqlite sensor data.")
    if os.path.exists("/tmp/sensors.sqlite"):
        os.remove("/tmp/sensors.sqlite")
    user_config = UserConfig(user_options)
    dao = SensorDao("/tmp/sensors.sqlite")
    sensors = Sensors(gridappsd=gapp, read_topic=read_topic, write_topic=write_topic,
                      user_config=user_config, sensor_store=dao)
    added = set()
    for meas_mrid, v in energy_measurements.items():
        try:
            if v['class'] == 'Analog':
                if len(user_config.sensors_config) > 0:
                    cfg = user_config.sensors_config.get(meas_mrid)
                    agg_int = cfg.get('aggregation-interval', user_config.aggregation_interval)
                    equipment = equipment_nomv[v['eqid']]
                    p = float(equipment['p'])
                    q = float(equipment['q'])
                    mag = float(math.sqrt(p ** 2 + q ** 2))
                    angle = math.degrees(math.atan(q / p))

                    # Always add magnitude first!
                    sensor = sensors.add_sensor(meas_mrid, mag, aggregation_interval=agg_int)
                    # note property sensors have the same aggregation period as the main sensor.
                    sensor.add_property_sensor("angle", math.atan(p / q))
                else:
                    # Only add one per measurement mrid
                    if meas_mrid in added:
                        continue
                    equipment = equipment_nomv[v['eqid']]
                    p = float(equipment['p'])
                    q = float(equipment['q'])
                    mag = float(math.sqrt(p**2 + q**2))
                    angle = math.degrees(math.atan(q / p))

                    # Always add magnitude first!
                    _log.debug(f"Adding {meas_mrid} {v['name']} to sensors")
                    sensor = sensors.add_sensor(meas_mrid, mag)
                    sensor.add_property_sensor("angle", math.atan(p/q))
                    added.add(meas_mrid)
            else:
                _log.debug(f"Skipping non-analog {v}")

        except KeyError:
            _log.error(f"Missing {v['eqid']} {v['name']} from equipment_nomv query")

    try:
        _log.info(f"Num sensors: {len(sensors._sensors)}")
        sensors.main_loop()
    except KeyboardInterrupt:
        pass
