import argparse
import calendar
from datetime import datetime
import json
import random
import time

from gridappsd import GridAPPSD
from gridappsd.topics import service_output_topic

class Sensor(object):
    def __init__(self, gridappsd, seed, allow_dropping, percent_error, output_topic):
        self._gapps = gridappsd
        self._allow_dropping = allow_dropping
        self._percent_error = percent_error
        self._seed = seed
        self._output_topic = output_topic
        random.seed(seed)

    @property
    def seed(self):
        return self._seed

    @property
    def allow_dropping(self):
        return self._allow_dropping

    @property
    def percent_error(self):
        return self._percent_error

    @property
    def output_topic(self):
        return self._output_topic

    def get_new_value(self, value):

        if self.allow_dropping:
            drop = random.uniform(0, 1)
            if drop > 0.99:
                return None

        if isinstance(value, bool):
            return value
        else:
            return random.uniform(0.99 * value, 1.01 * value)

    def __str__(self):
        return "seed: {}, percent error: {}, allow dropping: {}, output topic: {}".format(
            self.seed, self.percent_error, self.allow_dropping, self.output_topic
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
    # parser.add_argument("--percent-error", type=float, default=0.01,
    #                     help="Specify the percent error that is to be calculated.")
    parser.add_argument("--allow-dropping", type=bool, default=True,
                        help="Allow dropping of measurements.")
    parser.add_argument("-u", "--username", default="system",
                        help="The username to authenticate with the message bus.")
    parser.add_argument("-p", "--password", default="manager",
                        help="The password to authenticate with the message bus.")
    parser.add_argument("-a", "--stomp-address", default="127.0.0.1",
                        help="tcp address of the mesage bus.")
    parser.add_argument("--stomp-port", default=61613, type=int,
                        help="the stomp port on the message bus.")
    opts = parser.parse_args()

    opts.percent_error = 0.01

    return opts


if __name__ == '__main__':

    sensors = dict()
    opts = get_opts()

    read_topic = simulation_output_topic(opts.simulation_id)
    write_topic = service_output_topic("sensors", opts.simulation_id)

    gapp = GridAPPSD(username=opts.username,
                     password=opts.password,
                     stomp_address=opts.stomp_address,
                     stomp_port=opts.stomp_port)

    sensor = Sensor(gapp,
                    seed=opts.random_seed,
                    percent_error=opts.percent_error,
                    output_topic=write_topic,
                    allow_dropping=opts.allow_dropping)

    gapp.subscribe(read_topic, sensor.on_simulation_message)

    while True:
        time.sleep(0.1)

#
#    Subscribe to all messages on /topic/goss.gridappsd.simulation.output.<simulation_id>
#    Add 1% error from a uniform distribution to every numerical value received. One way of doing this, using Numpy, would be: import numpy as np; val_out = np.random.uniform (0.99 * val_in, 1.01 * val_in)
#    Publish each val_out on /topic/goss.gridappsd.simulation.sensors.<simulation_id>
#    Have an option to drop 1% of the publications from step 3. xmit = np.random.uniform (0, 1) and then publish if xmit <= 0.99:
