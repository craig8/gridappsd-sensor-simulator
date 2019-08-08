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

fh = logging.FileHandler("/tmp/sensors.log", mode='w', encoding='utf-8')
fh.setLevel(logging.DEBUG)
_log.addHandler(fh)


def get_opts():
    parser = argparse.ArgumentParser()

    # parser.add_argument("simulation_id",
    #                     help="Simulation id to use for responses on the message bus.")
    parser.add_argument("--random-seed", type=int, default=calendar.timegm(datetime.utcnow().timetuple()),
                        help="Seed for the random uniform distribution.")

    parser.add_argument("--sensors-config", default=None,
                        help="Configuration of sensors.")

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

    # Load the json file if there is a sensor config passed.
    if opts.sensors_config is not None:
        opts.sensors_config = json.loads(opts.sensors_config)

    return opts

def run_test (iname, oname, opts):
    ip = open (iname, 'r', newline='')

    # create a sensor for each input signal column
    rdr = csv.reader (ip, delimiter=',')
    colnames = next (rdr)
    colnames[0] = 't'
    ncol = len(colnames) - 1
    sensors = {}
    outnames = []
    outnames.append ('t')
    for i in range(ncol):
        column_name=colnames[i+1]
        sensors[i] = Sensor(None, 
                            seed=opts.random_seed,
                            nominal=opts.nominal,
                            perunit_confidence95=opts.perunit_confidence,
                            perunit_dropping=opts.perunit_dropping,
                            interval=opts.interval,
                            output_topic=column_name)
        outnames.append (column_name + '_avg')
        outnames.append (column_name + '_min')
        outnames.append (column_name + '_max')
    for i in sensors:
        print ('Sensor', i, '=', sensors[i])

    op = open (oname, 'w')
    wrt = csv.writer (op, delimiter=',')
    wrt.writerow (outnames)

    # write average, minimum and maximum for each sensor
    outputs = [0.0] * (3 * ncol + 1)
    # loop through the input rows, add samples, write the outputs
    for row in rdr:
        t = int(row[0])
        outputs[0] = t
        have_output = False
        for i in sensors:
            val = float(row[i+1])
            sensors[i].add_sample(t, val)
            if sensors[i].ready_to_sample(t):
                sample = sensors[i].take_range_sample(t)
                if sample[0] is not None:
                    outputs[3*i + 1] = sample[0]
                    outputs[3*i + 2] = sample[1]
                    outputs[3*i + 3] = sample[2]
                else:
                    outputs[3*i + 1] = 0.0
                    outputs[3*i + 2] = 0.0
                    outputs[3*i + 3] = 0.0
                have_output = True
        if have_output:
            wrt.writerow (['{:.3f}'.format(x) for x in outputs])

    ip.close()
    op.close()


if __name__ == '__main__':

    sensors = dict()
    opts = get_opts()

    if opts.simulation_id == '-9999':
        print('entering test mode with', opts)
        run_test ('Input.csv', 'Output.csv', opts)
        raise SystemExit

    gapp = GridAPPSD(username=opts.username,
                     password=opts.password,
                     address=opts.address)

    read_topic = simulation_output_topic(opts.simulation_id)
    write_topic = service_output_topic("sensors", opts.simulation_id)

    sensors = Sensors(gapp, seed=opts.random_seed, sensor_config=opts.sensors_config)
    sensors.main_loop()

    #
    # sensor = Sensor(gapp,
    #                 seed=opts.random_seed,
    #                 nominal=opts.nominal, # TODO (Craig, Tom, Andy F): these 4 parameters will need to be different for each sensor instance
    #                 perunit_confidence95=opts.perunit_confidence,
    #                 perunit_dropping=opts.perunit_dropping,
    #                 interval=opts.interval,
    #                 input_topic=read_topic,
    #                 output_topic=write_topic)

    sensor.main_loop()

#
#    Subscribe to all messages on /topic/goss.gridappsd.simulation.output.<simulation_id>
#    Add 1% error from a uniform distribution to every numerical value received. One way of doing this, using Numpy, would be: import numpy as np; val_out = np.random.uniform (0.99 * val_in, 1.01 * val_in)
#    Publish each val_out on /topic/goss.gridappsd.simulation.sensors.<simulation_id>
#    Have an option to drop 1% of the publications from step 3. xmit = np.random.uniform (0, 1) and then publish if xmit <= 0.99:
