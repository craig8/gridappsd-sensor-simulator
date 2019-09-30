from copy import deepcopy
import json
import logging
import random
import time

_log = logging.getLogger(__file__)

DEFAULT_SENSOR_CONFIG = {
    "default-perunit-confidence-rate": 0.01,
    "default-aggregation-interval": 30,
    "default-perunit-drop-rate": 0.01,
    'default-nominal-voltage': 100
}


class Sensors(object):
    def __init__(self, gridappsd, read_topic, write_topic, log_statistics=False, user_options=None):
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
        :param random_seed:
            A random seed to be used for reproducible results.
        :param sensor_config:
            Configuration of the sensors that should have noise to them.
        """
        super(Sensors, self).__init__()
        if user_options is None:
            user_options = {}
        else:
            user_options = deepcopy(user_options)
        self._random_seed = user_options.get('random-seed', 0)
        self._sensors = {}
        self._gappsd = gridappsd
        self._read_topic = read_topic
        self._write_topic = write_topic
        self._log_statistics = log_statistics

        assert self._gappsd, "Invalid gridappsd object specified, cannot be None"
        assert self._read_topic, "Invalid read topic specified, cannot be None"
        assert self._write_topic, "Invalid write topic specified, cannob be None"

        sensors_config = user_options.pop("sensors-config", {})
        self.passthrough_if_not_specified = user_options.pop('passthrough-if-not-specified', True)
        self.default_perunit_confifidence_rate = user_options.get('default-perunit-confidence-rate',
                                                                  DEFAULT_SENSOR_CONFIG['default-perunit-confidence-rate'])
        self.default_drop_rate = user_options.get("default-perunit-drop-rate",
                                                  DEFAULT_SENSOR_CONFIG['default-perunit-drop-rate'])
        self.default_aggregation_interval = user_options.get("default-aggregation-interval",
                                                             DEFAULT_SENSOR_CONFIG['default-aggregation-interval'])
        self.default_nominal_voltage = user_options.get('default-nominal-voltage',
                                                        DEFAULT_SENSOR_CONFIG['default-nominal-voltage'])
        for k, v in sensors_config.items():
            agg_interval = v.get("aggregation-interval", self.default_aggregation_interval)
            perunit_drop_rate = v.get("drop-rate", self.default_drop_rate)
            perunit_confidence_rate = v.get('per-unit-confidence-rate', self.default_perunit_confifidence_rate)
            nominal_voltage = v.get('nominal-voltage', self.default_nominal_voltage)
            self._sensors[k] = Sensor(self._random_seed, nominal_voltage=100, aggregation_interval=agg_interval,
                                      perunit_drop_rate=perunit_drop_rate,
                                      perunit_confidence_rate=perunit_confidence_rate)

    def on_simulation_message(self, headers, message):
        """ Listens for simulation messages off the gridappsd message bus

        If the message is a measurements message then the function will inspect each
        non string value and generate a new value for the passed data.

        :param headers:
        :param message:
        :return:
        """

        configured_sensors = set(self._sensors.keys())

        measurement_out = {}

        if self.passthrough_if_not_specified:
            measurement_out = deepcopy(message['message']['measurements'])

        timestep = message['message']['timestamp']

        for mrid in configured_sensors:
            new_measurement = dict(
                measurement_mrid=mrid
            )

            item = message['message']['measurements'][mrid]

            # Create new values for data from the sensor.
            for prop, value in item.items():
                # TODO: this only processes 'magnitude' and 'value'
                #       it needs to also process 'angle' but with different noise
                if prop in ('measurement_mrid', 'angle'):
                    new_measurement[prop] = value
                    continue
                new_value = self._sensors[mrid].get_new_value(timestep, value)
                new_measurement[prop] = new_value

            measurement_out[mrid] = new_measurement
            # measurement_mrid = item['measurement_mrid']
            # if measurement_mrid in configured_sensors:
            #     # replace measurement value based upon the sensor configuration.
            #     new_measurement = {}
            #     # Create new values for data from the sensor.
            #     for prop, value in item.items():
            #         # TODO: this only processes 'magnitude' and 'value'
            #         #       it needs to also process 'angle' but with different noise
            #         if prop in ('measurement_mrid', 'angle'):
            #             new_measurement[prop] = value
            #             continue
            #         new_value = self._sensors[measurement_mrid].get_new_value(timestep, value)
            #         new_measurement[prop] = new_value
            #
            #     # Gent new values for things
            #     configured_sensors.remove(measurement_mrid)
            #     output_measurements[measurement_mrid] = new_measurement
            # else:
            #     # pass through
            #     output_measurements[measurement_mrid] = item

        message['message']['measurements'] = measurement_out
        _log.debug(f"Sending to: {self._write_topic}\nmessage: {message}")
        self._gappsd.send(self._write_topic, json.dumps(message))

    def main_loop(self):
        self._gappsd.subscribe(self._read_topic, self.on_simulation_message)

        while True:
            time.sleep(0.001)


class Sensor(object):
    def __init__(self, seed, nominal_voltage, aggregation_interval, perunit_drop_rate, perunit_confidence_rate):
        """

        :param gridappsd:
        :param seed:
        :param nominal:
        :param perunit_dropping:
        :param perunit_confidence:
        :param interval:
        :param input_topic:
        """
        self._nominal = nominal_voltage
        self._perunit_dropping = perunit_drop_rate
        self._stddev = nominal_voltage * perunit_confidence_rate / 1.96  # for normal distribution
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
        return "seed: {}, nominal: {}, stddev: {}, pu dropped: {}, agg interval: {}".format(
            self.seed, self.nominal, self.stddev, self.perunit_dropping, self.interval
        )
