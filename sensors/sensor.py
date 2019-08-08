from copy import deepcopy
import json
import logging
import random
import time

_log = logging.getLogger(__file__)

DEFAULT_SENSOR_CONFIG = {
    "nominal-voltage": 100,
    "perunit-confidence": 0.01,
    "aggregation-interval": 30,
    "perunit-dropping": 0.01
}


class Sensors(object):
    def __init__(self, gridappsd, read_topic, write_topic, log_statistics=False, seed=None, sensor_config={}):
        """ initializes sensor objects

        sensor_config should be a dictionary of dictionaries to handle
        the specification of different parameters for individual sensors

        Each value of the dictionary should have the following attributes specified.  If
        no value is supplied then the defaults listed in DEFAULT_SENSOR_CONFIG will be used

            - nominal-voltage:      The nominal voltage of the sensor
            - aggregation-interval: Number of seconds to aggregate for the sensor
            - perunit-dropping:
            - perunit-confidence:

        Example sensor_config:

            {
                "61A547FB-9F68-5635-BB4C-F7F537FD824E": {
                    "nominal-voltage": 100,
                    "perunit-confidence": 0.01
                    "aggregation-interval": 30,
                    "perunit-dropping": 0.01
                }
                ...
            }

        :param read_topic:
            The topic to listen for measurement data to come through the bus
        :param write_topic
            The topic to write the measurement data to
        :param log_statistics
            Boolean: Log statistics to the gridappsd log
        :param gridappsd:
            The main object used to connect to gridappsd
        :param seed:
            A random seed to be used for reproducible results.
        :param sensor_config:
            Configuration of the sensors that should have noise to them.
        """
        super(Sensors, self).__init__()
        self._seed = seed
        self._sensors = {}
        self._gappsd = gridappsd
        self._read_topic = read_topic
        self._write_topic = write_topic
        self._log_statistics = log_statistics

        assert self._gappsd, "Invalid gridappsd object specified, cannot be None"
        assert self._read_topic, "Invalid read topic specified, cannot be None"
        assert self._write_topic, "Invalid write topic specified, cannob be None"

        for k, v in sensor_config.items():
            values = DEFAULT_SENSOR_CONFIG.copy()
            values.update(v)
            arg_values = {}
            for k1, v1 in values.items():
                arg_values[k1.replace("-", "_")] = v1
            self._sensors[k] = Sensor(self._seed, **arg_values)

    def on_simulation_message(self, headers, message):
        """ Listens for simulation messages off the gridappsd message bus

        If the message is a measurements message then the function will inspect each
        non string value and generate a new value for the passed data.

        :param headers:
        :param message:
        :return:
        """

        configured_sensors = set(self._sensors.keys())

        obj_original = deepcopy(message)
        obj_out = deepcopy(message)
        timestep = obj_original['message']['timestamp']
        # Since measurements are an array we use this as an
        # array of the output.
        #
        # Note an inplace replacement of data would be more efficient, but
        # for now we will use a loop through all the measurements.
        obj_measurement_out = []

        for measurement in obj_original['message']['measurements']:
            measurement_mrid = measurement['measurement_mrid']
            if measurement_mrid in configured_sensors:
                # replace measurement value based upon the sensor configuration.
                new_measurement = {}
                # Create new values for data from the sensor.
                for prop, value in measurement.items():
                    # TODO: this only processes 'magnitude' and 'value'
                    #       it needs to also process 'angle' but with different noise
                    if prop in ('measurement_mrid', 'angle'):
                        new_measurement[prop] = value
                        continue
                    new_value = self._sensors[measurement_mrid].get_new_value(timestep, value)
                    new_measurement[prop] = new_value

                # Gent new values for things
                configured_sensors.remove(measurement_mrid)
                obj_measurement_out.append(new_measurement)
            else:
                # pass through
                obj_measurement_out.append(measurement)

        obj_out['message']['measurements'] = obj_measurement_out
        # Publish new data back out to the message bus
        self._gappsd.send(self._write_topic, json.dumps(obj_out))

    def main_loop(self):
        self._gappsd.subscribe(self._read_topic, self.on_simulation_message)

        while True:
            time.sleep(0.001)


class Sensor(object):
    def __init__(self, seed, nominal, aggregation_interval, perunit_dropping, perunit_confidence95):
        """

        :param gridappsd:
        :param seed:
        :param nominal:
        :param perunit_dropping:
        :param perunit_confidence95:
        :param interval:
        :param input_topic:
        :param output_topic:
        """
        self._nominal = nominal
        self._perunit_dropping = perunit_dropping
        self._stddev = nominal * perunit_confidence95 / 1.96  # for normal distribution
        self._seed = seed
        self._interval = aggregation_interval
        # Set default - Uninitialized values for internal properties.
        self._n = 0
        self._tstart = 0
        self._average = 0
        self._min = 0
        self._max = 0
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
            offset = random.randint(0, self._interval - 1)  # each sensor needs a staggered start
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
                return None, None, None
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

    def __str__(self):
        return "seed: {}, nominal: {}, stddev: {}, pu dropped: {}, agg interval: {}, output topic: {}".format(
            self.seed, self.nominal, self.stddev, self.perunit_dropping, self.interval, self.output_topic
        )
