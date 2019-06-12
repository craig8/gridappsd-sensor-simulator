import argparse
import calendar
from datetime import datetime
import json
import random
import time
import sys
import csv
import logging

from gridappsd import GridAPPSD, utils
from gridappsd.topics import service_output_topic, simulation_output_topic

_log = logging.getLogger(__name__)
_log.setLevel(logging.DEBUG)

fh = logging.FileHandler("/tmp/debug/sensors.log")
fh.setLevel(logging.DEBUG)
_log.addHandler(fh)

class Sensor(object):
    def __init__(self, gridappsd, seed, nominal, perunit_dropping, perunit_confidence95, interval, output_topic):
        self._gapps = gridappsd
        self._nominal = nominal
        self._perunit_dropping = perunit_dropping
        self._stddev = nominal * perunit_confidence95 / 1.96  # for normal distribution
        self._seed = seed
        self._interval = interval
        self._output_topic = output_topic
        self._initialized = False
        random.seed(seed)
        _log.debug(self)

    def __repr__(self):
        return f"""
<Sensor(seed={self.seed}, nominal={self.nominal}, interval={self.interval}, output_topic={self.output_topic},
        perunit_dropping={self.perunit_dropping})>""" 

    @property
    def seed(self):
        return self._seed

    @property
    def nominal(self):
        return self._nominal

    @property
    def perunit_dropping(self):
        return self._perunit_dropping

    @property
    def stddev(self):
        return self._stddev

    @property
    def interval(self):
        return self._interval

    @property
    def output_topic(self):
        return self._output_topic

    def initialize(self, t, val):
        if self._interval > 0.0:
            offset = random.randint (0, self._interval - 1) # each sensor needs a staggered start
            self.reset_interval (t - offset, val)
        else:
            self._n = 1
            self._tstart = t
            self._average = val
            self._min = val
            self._max = val
        self._initialized = True

    def reset_interval(self, t, val):
        self._n = 1
        self._tstart = t
        self._average = val
        self._min = val # sys.float_info.max
        self._max = val # -sys.float_info.max

    def add_sample(self, t, val):
        if not self._initialized:
            self.initialize (t, val)
        if t - self._tstart <= self._interval:
            if val < self._min:
                self._min = val
            if val > self._max:
                self._max = val
            self._average = self._average + val
            self._n = self._n + 1

    def ready_to_sample(self, t):
        if t >= self._tstart + self._interval:
            return True
        return False

    def take_range_sample(self, t):
        if self._n < 1:
            self._n = 1
        mean_val = self._average / self._n
        if self.perunit_dropping > 0.0:
            drop = random.uniform(0, 1)
            if drop <= self.perunit_dropping:
                self.reset_interval(t, mean_val)
                return (None, None, None)
        ret = (mean_val + random.gauss(0.0, self._stddev), # TODO (Tom, Andy, Andy): do we want the same error on each?
               self._min + random.gauss(0.0, self._stddev), 
               self._max + random.gauss(0.0, self._stddev))
        self.reset_interval(t, mean_val)
        return ret

    def take_inst_sample(self, t):
        if self._n < 1:
            self._n = 1
        mean_val = self._average / self._n
        if self.perunit_dropping > 0.0:
            drop = random.uniform(0, 1)
            if drop <= self.perunit_dropping:
                self.reset_interval(t, mean_val)
                return None
        ret = mean_val + random.gauss(0.0, self._stddev)
        self.reset_interval(t, mean_val)
        return ret

    def get_new_value(self, t, value):
        self.add_sample (t, value)
        if self.ready_to_sample (t):
            return self.take_inst_sample(t)
        return None
#       if self.perunit_dropping > 0.0:
#           drop = random.uniform(0, 1)
#           if drop <= self.perunit_dropping:
#               return None
#
#       if isinstance(value, bool) or self._stddev <= 0.0:
#           return value
#       else:
#           return value + random.gauss(0.0, self._stddev)

    def __str__(self):
        return "seed: {}, nominal: {}, stddev: {}, pu dropped: {}, agg interval: {}, output topic: {}".format(
            self.seed, self.nominal, self.stddev, self.perunit_dropping, self.interval, self.output_topic
        )

    def on_simulation_message(self, headers, message):
        """ Listens for simulation messages off the gridappsd message bus

        If the message is a measurements message then the function will inspect each
        non string value and generate a new value for the passed data.

        :param headers:
        :param message:
        :return:
        """
        mobj = message.copy()
        mout = mobj.copy()
        mout['measurements'] = []

        t = mobj['message']['timestamp'] 
        _log.debug(f"Processing Timestamp: {t}")
        for measurement in mobj['message']['measurements']:
            remove = False
            mcopy = measurement.copy()
            for prop, value in mcopy.items():
                # TODO: this only processes 'magnitude' and 'value'
                #       it needs to also process 'angle' but with different noise
                if prop in ('measurement_mrid', 'angle'):
                    continue

                new_value = self.get_new_value(t, value)

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
    parser.add_argument("--nominal", type=float, default=100.0,
                        help="Specify the nominal range of sensor measurements.")
    parser.add_argument("--perunit-confidence", type=float, default=0.01,
                        help="Specify the 95% confidence interval, in +/- perunit of nominal range.")
    parser.add_argument("--perunit-dropping", type=float, default=0.01,
                        help="Fraction of measurements that are not republished.")
    parser.add_argument("--interval", type=float, default=30.0,
                        help="Interval in seconds for min, max, average aggregation.")
    parser.add_argument("-u", "--username", default=utils.get_gridappsd_user(),
                        help="The username to authenticate with the message bus.")
    parser.add_argument("-p", "--password", default=utils.get_gridappsd_pass(),
                        help="The password to authenticate with the message bus.")
    parser.add_argument("-a", "--address", default=utils.get_gridappsd_address(),
                        help="The tcp://addr:port that gridappsd is located on.") 
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
        print ('entering test mode with', opts)
        run_test ('Input.csv', 'Output.csv', opts)
        raise SystemExit

    read_topic = simulation_output_topic(opts.simulation_id)
    write_topic = service_output_topic("sensors", opts.simulation_id)

    gapp = GridAPPSD(username=opts.username,
                     password=opts.password,
                     address=opts.address)

    sensor = Sensor(gapp,
                    seed=opts.random_seed,
                    nominal=opts.nominal, # TODO (Craig, Tom, Andy F): these 4 parameters will need to be different for each sensor instance
                    perunit_confidence95=opts.perunit_confidence,
                    perunit_dropping=opts.perunit_dropping,
                    interval=opts.interval,
                    output_topic=write_topic)

    gapp.subscribe(read_topic, sensor.on_simulation_message)

    while True:
        time.sleep(0.1)

#
#    Subscribe to all messages on /topic/goss.gridappsd.simulation.output.<simulation_id>
#    Add 1% error from a uniform distribution to every numerical value received. One way of doing this, using Numpy, would be: import numpy as np; val_out = np.random.uniform (0.99 * val_in, 1.01 * val_in)
#    Publish each val_out on /topic/goss.gridappsd.simulation.sensors.<simulation_id>
#    Have an option to drop 1% of the publications from step 3. xmit = np.random.uniform (0, 1) and then publish if xmit <= 0.99:
