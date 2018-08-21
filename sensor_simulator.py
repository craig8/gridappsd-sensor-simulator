import argparse
import calendar
from datetime import datetime
import json
import random
import time
import sys
import csv

from gridappsd import GridAPPSD
from gridappsd.topics import simulation_output_topic


def sensor_output_topic(simulation_id):
    """ create an output topic for the sensor to write to.

    The output topic will be based upon the main simulation_output_topic.

    :param simulation_id:
    :return:
    """
    original = simulation_output_topic(simulation_id)
    partitioned = original.split('.')
    new_topic = '.'.join(partitioned[:-2] + ['sensors'] + [partitioned[-1]])
    return new_topic


class Sensor(object):
    def __init__(self, gridappsd, seed, perunit_dropping, perunit_error, interval, output_topic):
        self._gapps = gridappsd
        self._perunit_dropping = perunit_dropping
        self._perunit_error = perunit_error
        self._seed = seed
        self._interval = interval
        self._output_topic = output_topic
        random.seed(seed)

    @property
    def seed(self):
        return self._seed

    @property
    def perunit_dropping(self):
        return self._perunit_dropping

    @property
    def perunit_error(self):
        return self._perunit_error

    @property
    def interval(self):
        return self._interval

    @property
    def output_topic(self):
        return self._output_topic

    def reset_interval(self, t):
        self._n = 0
        self._tstart = t
        self._average = 0.0
        self._min = sys.float_info.max
        self._max = -sys.float_info.max

    def add_sample(self, t, val):
        if t - self._tstart <= self._interval:
            if val < self._min:
                self._min = val
            if val > self._max:
                self._max = val
            self._average = self._average + val
            self._n = self._n + 1

    def check_sample(self, t):
        if t >= self._tstart + self._interval:
            return True
        return False

    def take_sample(self, t):
        ret = (self._average / self._n, self._min, self._max)
        self.reset_interval(t)
        return ret

    def get_new_value(self, value):

        if self.perunit_dropping > 0.0:
            drop = random.uniform(0, 1)
            if drop <= self.perunit_dropping:
                return None

        if isinstance(value, bool) or self.perunit_error <= 0.0:
            return value
        else: #TODO define error bounds around a nominal value or nominal range, not around the instantaneous value
            band = self.perunit_error * value
            return random.uniform(value - band, value + band)

    def __str__(self):
        return "seed: {}, perunit error: {}, perunit dropping: {}, interval: {}, output topic: {}".format(
            self.seed, self.perunit_error, self.perunit_dropping, self.interval, self.output_topic
        )

    def on_simulation_message(self, headers, message):
        """ Listens for simulation messages off the gridappsd message bus

        If the message is a measurements message then the function will inspect each
        non string value and generate a new value for the passed data.

        :param headers:
        :param message:
        :return:
        """

        mobj = json.loads(message)
        mout = mobj.copy()
        mout['measurements'] = []

        for measurement in mobj['measurements']:
            remove = False
            mcopy = measurement.copy()
            for prop, value in mcopy.items():
                if isinstance(value, str):
                    # continue on as strings won't be modified.
                    continue

                new_value = self.get_new_value(value)

                if not new_value:
                    remove = True
                else:
                    mcopy[prop] = new_value

            if not remove:
                mout['measurements'].append(mcopy)

        # Publish new data back out to the message bus
        self._gapps.send(self.output_topic, json.dumps(mout))


def get_opts():
    parser = argparse.ArgumentParser()

    parser.add_argument("simulation_id",
                        help="Simulation id to use for responses on the message bus.")
    parser.add_argument("--random-seed", type=int, default=calendar.timegm(datetime.utcnow().timetuple()),
                        help="Seed for the random uniform distribution.")
    parser.add_argument("--perunit-error", type=float, default=0.01,
                        help="Specify the perunit error that is to be calculated.")
    parser.add_argument("--perunit-dropping", type=float, default=0.0001,
                        help="Fraction of measurements that are not republished.")
    parser.add_argument("--interval", type=float, default=900.0,
                        help="Interval in seconds for min, max, average aggregation.")
    parser.add_argument("-u", "--username", default="system",
                        help="The username to authenticate with the message bus.")
    parser.add_argument("-p", "--password", default="manager",
                        help="The password to authenticate with the message bus.")
    parser.add_argument("-a", "--stomp-address", default="127.0.0.1",
                        help="tcp address of the mesage bus.")
    parser.add_argument("--stomp-port", default=61613, type=int,
                        help="the stomp port on the message bus.")
    opts = parser.parse_args()
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
                        perunit_error=opts.perunit_error,
                        perunit_dropping=opts.perunit_dropping,
                        interval=opts.interval,
                        output_topic=column_name)
        outnames.append (column_name + '_avg')
        outnames.append (column_name + '_min')
        outnames.append (column_name + '_max')
    for i in sensors:
        print ('Sensor', i, '=', sensors[i])
        sensors[i].reset_interval (0.0)

    op = open (oname, 'w')
    wrt = csv.writer (op, delimiter=',')
    wrt.writerow (outnames)

    # write average, minimum and maximum for each sensor
    outputs = [0.0] * (3 * ncol + 1)
    # loop through the input rows, add samples, write the outputs
    for row in rdr:
        t = float(row[0])
        outputs[0] = t
        have_output = False
        for i in sensors:
            val = float(row[i+1])
            sensors[i].add_sample(t, val)
            if sensors[i].check_sample(t):
                sample = sensors[i].take_sample(t)
                outputs[3*i + 1] = sample[0]
                outputs[3*i + 2] = sample[1]
                outputs[3*i + 3] = sample[2]
                have_output = True
        if have_output:
            wrt.writerow (['{:.3f}'.format(x) for x in outputs])

    ip.close()
    op.close()

if __name__ == '__main__':

    sensors = dict()
    opts = get_opts()

    if opts.simulation_id == '-9999':
        print ('entering test mode with', opts)
        run_test ('Input.csv', 'Output.csv', opts)
        raise SystemExit

    read_topic = simulation_output_topic(opts.simulation_id)
    write_topic = sensor_output_topic(opts.simulation_id)

    gapp = GridAPPSD(username=opts.username,
                     password=opts.password,
                     stomp_address=opts.stomp_address,
                     stomp_port=opts.stomp_port)

    sensor = Sensor(gapp,
                    seed=opts.random_seed,
                    perunit_error=opts.perunit_error,
                    output_topic=write_topic,
                    perunit_dropping=opts.perunit_dropping,
                    interval=opts.interval)

    gapp.subscribe(read_topic, sensor.on_simulation_message)

    while True:
        time.sleep(0.1)

#
#    Subscribe to all messages on /topic/goss.gridappsd.simulation.output.<simulation_id>
#    Add 1% error from a uniform distribution to every numerical value received. One way of doing this, using Numpy, would be: import numpy as np; val_out = np.random.uniform (0.99 * val_in, 1.01 * val_in)
#    Publish each val_out on /topic/goss.gridappsd.simulation.sensors.<simulation_id>
#    Have an option to drop 1% of the publications from step 3. xmit = np.random.uniform (0, 1) and then publish if xmit <= 0.99:
