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
    def __init__(self, gridappsd, read_topic, write_topic, user_options: dict = None):
        """
        Create sensors based upon thee user_options dictionary

        The user_options dictionary will have the following structure:
            {
                "sensors-config": {
                    "_001cc221-d6e6-485d-bdcc-b84cb643d1ec": {
                        "nominal-voltage": 100,
                        "perunit-confidence-rate": 0.01,
                        "aggregation-interval": 30,
                        "perunit-drop-rate": 0.01
                    },
                    "_0031ff7c-5140-47cf-b750-0146bb3d9024": {},
                    "_00313f7c-5140-47cf-b750-0146bb3d9024": {
                        "nominal-voltage": 35
                    },
                    "default-perunit-confidence-rate": 0.01,
                    "default-aggregation-interval": 30,
                    "default-perunit-drop-rate": 0.01,
                    "passthrough-if-not-specified": true,
                    "random-seed": 0,
                    "log-statistics": false
                }
            }

            sensor-config - contains a dictionary of sensors the service will enhance with noise and/or
                            drop from existence based upon parameters.

                            For each sensor one can specify one or more of the following properties:

                                nominal-voltage         - Normal voltage level for the sensor (note this will become
                                                          automated when querying for this from blazegraph is
                                                          implemented)
                                perunit-confidence-rate - Confidence level that the mean value is within this range
                                aggregation-interval    - Number of samples to collect before emitting a measurement
                                perunit-drop-rate       - Rate to drop the measurement value

            random-seed - A seed to produce reliable results over different runs of the code base
            passthrough-if-not-specified - Allows measurements of non-specified sensors to be published to the
                                           sensors output topic without modification.

            The following values are used as defaults for each sensor listed in sensor-config but does not specify
            the value for the parameter

                default-perunit-confidence-rate
                default-aggregation-interval
                default-perunit-drop-rate

        :param read_topic:
            The topic to listen for measurement data to come through the bus
        :param write_topic
            The topic to write the measurement data to
        :param gridappsd:
            The main object used to connect to gridappsd
        :param user_options:
            A dictionary of options to specify how the service will run.
        """
        super(Sensors, self).__init__()
        if user_options is None:
            user_options = {}
        else:
            user_options = deepcopy(user_options)
        self._random_seed = user_options.get('random-seed', 0)
        self._sensors = {}
        self._gappsd = gridappsd
        self._logger = self._gappsd.get_logger()
        self._read_topic = read_topic
        self._write_topic = write_topic
        self._log_statistics = False

        assert self._gappsd, "Invalid gridappsd object specified, cannot be None"
        assert self._read_topic, "Invalid read topic specified, cannot be None"
        assert self._write_topic, "Invalid write topic specified, cannob be None"

        sensors_config = user_options.pop("sensors-config", {})
        self.passthrough_if_not_specified = user_options.pop('passthrough-if-not-specified', True)
        self.default_perunit_confifidence_rate = user_options.get('default-perunit-confidence-rate',
                                                                  DEFAULT_SENSOR_CONFIG[
                                                                      'default-perunit-confidence-rate'])
        self.default_drop_rate = user_options.get("default-perunit-drop-rate",
                                                  DEFAULT_SENSOR_CONFIG['default-perunit-drop-rate'])
        self.default_aggregation_interval = user_options.get("default-aggregation-interval",
                                                             DEFAULT_SENSOR_CONFIG['default-aggregation-interval'])
        self.default_nominal_voltage = user_options.get('default-nominal-voltage',
                                                        DEFAULT_SENSOR_CONFIG['default-nominal-voltage'])
        for k, v in sensors_config.items():
            agg_interval = v.get("aggregation-interval", self.default_aggregation_interval)
            perunit_drop_rate = v.get("perunit-drop-rate", self.default_drop_rate)
            perunit_confidence_rate = v.get('perunit-confidence-rate', self.default_perunit_confifidence_rate)
            nominal_voltage = v.get('nominal-voltage', self.default_nominal_voltage)
            self._sensors[k] = Sensor(self._random_seed, nominal_voltage=nominal_voltage,
                                      aggregation_interval=agg_interval,
                                      perunit_drop_rate=perunit_drop_rate,
                                      perunit_confidence_rate=perunit_confidence_rate)

        _log.debug("Created {} sensors".format(len(self._sensors)))

    def on_simulation_message(self, headers, message):
        """
        Listen for simulation measurement messages off the gridappsd message bus.

        Each measurement is mapped onto a `Sensor` which determines whether or not the
        measurement is published to the sensor output topic or dropped.

        :param headers:
        :param message:
            Simulation measurement message.
        """

        configured_sensors = set(self._sensors.keys())

        measurement_out = {}

        # if passthrough set then copy over the measurmments of the entire message
        # into the output.
        if self.passthrough_if_not_specified:
            measurement_out = deepcopy(message['message']['measurements'])

        timestamp = message['message']['timestamp']

        # Loop over the configured sensor andding measurements for each of them
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

                new_value = self._sensors[mrid].get_new_value(timestamp, value)
                if new_value is None:
                    new_measurement = None
                    break

                new_measurement[prop] = new_value

            if new_measurement is not None:
                measurement_out[mrid] = new_measurement

        if len(measurement_out) > 0:
            message['message']['measurements'] = measurement_out
            if self._log_statistics:
                self._log_sensors()

            self._gappsd.send(self._write_topic, json.dumps(message))

    def _log_sensors(self):
        for s in self._sensors:
            _log.debug(s)
            self._logger.debug(s)

    def main_loop(self):
        self._gappsd.subscribe(self._read_topic, self.on_simulation_message)

        while True:
            time.sleep(0.001)


class Sensor(object):
    def __init__(self, random_seed, nominal_voltage, aggregation_interval, perunit_drop_rate, perunit_confidence_rate):
        """
        An object modeling an individual sensor.

        :param random_seed:
        :param nominal_voltage:
        :param aggregation_interval:
        :param perunit_drop_rate:
        :param perunit_confidence_rate:
        """
        self._nominal = nominal_voltage
        self._perunit_dropping = perunit_drop_rate
        self._stddev = nominal_voltage * perunit_confidence_rate / 1.96  # for normal distribution
        self._seed = random_seed
        self._interval = aggregation_interval
        # Set default - Uninitialized values for internal properties.
        self._n = 0
        self._tstart = 0
        self._average = 0
        self._min = 0
        self._max = 0
        self._initialized = False

        random.seed(random_seed)
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
            self.reset_interval(t - offset, val)
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
        self._min = val  # sys.float_info.max
        self._max = val  # -sys.float_info.max

    def add_sample(self, t, val):
        if not self._initialized:
            self.initialize(t, val)
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
        ret = (mean_val + random.gauss(0.0, self._stddev),  # TODO (Tom, Andy, Andy): do we want the same error on each?
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
        self.add_sample(t, value)
        if self.ready_to_sample(t):
            return self.take_inst_sample(t)
        return None

    def __str__(self):
        return "seed: {}, nominal: {}, stddev: {}, pu dropped: {}, agg interval: {}".format(
            self.seed, self.nominal, self.stddev, self.perunit_dropping, self.interval
        )
