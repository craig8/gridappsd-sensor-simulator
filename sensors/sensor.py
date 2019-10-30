from copy import deepcopy
import json
import logging
import sys
import random
import time

from .measurements import get_sensors_config

_log = logging.getLogger(__name__)

_log.setLevel(logging.DEBUG)
DEFAULT_SENSOR_CONFIG = {
    "default-perunit-confidence-band": 2,
    "default-aggregation-interval": 30,
    "default-perunit-drop-rate": 0.01,
    'default-normal-value': 100
}


class Sensors(object):
    def __init__(self, gridappsd, read_topic, write_topic, feeder, user_options: dict = None):
        """
        Create sensors based upon thee user_options dictionary

        The user_options dictionary will have the following structure:
            {
                "sensors-config": {
                    "_001cc221-d6e6-485d-bdcc-b84cb643d1ec": {
                        "normal-value": 100,
                        "perunit-confidence-band": 0.01,
                        "aggregation-interval": 30,
                        "perunit-drop-rate": 0.01
                    },
                    "_0031ff7c-5140-47cf-b750-0146bb3d9024": {},
                    "_00313f7c-5140-47cf-b750-0146bb3d9024": {
                        "normal-value": 35
                    },
                    "default-perunit-confidence-band": 0.01,
                    "default-aggregation-interval": 30,
                    "default-perunit-drop-rate": 0.01,
                    "passthrough-if-not-specified": false,
                    "random-seed": 0,
                    "log-statistics": false,
                    "simulate-all": false
                }
            }

            sensor-config - contains a dictionary of sensors the service will enhance with noise and/or
                            drop from existence based upon parameters.

                            For each sensor one can specify one or more of the following properties:

                                nominal-voltage         - Normal voltage level for the sensor (note this will become
                                                          automated when querying for this from blazegraph is
                                                          implemented)
                                perunit-confidence-band - Confidence level that the mean value is within this range
                                aggregation-interval    - Number of samples to collect before emitting a measurement
                                perunit-drop-rate       - Rate to drop the measurement value

            random-seed - A seed to produce reliable results over different runs of the code base
            passthrough-if-not-specified - Allows measurements of non-specified sensors to be published to the
                                           sensors output topic without modification.

            The following values are used as defaults for each sensor listed in sensor-config but does not specify
            the value for the parameter

                default-perunit-confidence-band
                default-aggregation-interval
                default-perunit-drop-rate

        :param read_topic:
            The topic to listen for measurement data to come through the bus
        :param write_topic
            The topic to write the measurement data to
        :param gridappsd:
            The main object used to connect to gridappsd
        :param feeder:
            The feeder model that is being used in this simulation.
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
        self.passthrough_if_not_specified = user_options.pop('passthrough-if-not-specified', False)
        self.simulate_all = user_options.pop('simulate-all', False)
        self.default_perunit_confifidence_band = user_options.get('default-perunit-confidence-band',
                                                                  DEFAULT_SENSOR_CONFIG[
                                                                            'default-perunit-confidence-band'])
        self.default_drop_rate = user_options.get("default-perunit-drop-rate",
                                                  DEFAULT_SENSOR_CONFIG['default-perunit-drop-rate'])
        self.default_aggregation_interval = user_options.get("default-aggregation-interval",
                                                             DEFAULT_SENSOR_CONFIG['default-aggregation-interval'])
        self.default_normal_value = user_options.get('default-normal-value',
                                                     DEFAULT_SENSOR_CONFIG['default-normal-value'])
        if self.simulate_all:
            sensors_config = get_sensors_config(feeder)

        # _log.debug(f"sensors_config is: {sensors_config}")
        random.seed(self._random_seed)
        _log.debug("measurement_id,normal_value,class,type,power,eqtype")
        for k, v in sensors_config.items():
            cn_nomv = ''
            try:
                has_cn_pnv = True
                cn_nomv = v['cn_nomv']
            except KeyError:
                has_cn_pnv = False

            amp = ''
            eqtype = ''
            try:
                has_va = True
                amp = v['current_nomv']['val']
                eqtype = v['current_nomv']['eqtype']
                va_normal = v['current_nomv']['va_normal']
            except KeyError:
                has_va = False

            normal_value = None
            if v['type'] == 'VA' and has_va:
                normal_value = va_normal
            elif v['type'] == 'PNV' and has_cn_pnv:
                normal_value = cn_nomv
            agg_interval = v.get("aggregation-interval", self.default_aggregation_interval)
            perunit_drop_rate = v.get("perunit-drop-rate", self.default_drop_rate)
            perunit_confidence_rate = v.get('perunit-confidence-band', self.default_perunit_confifidence_band)

            if not normal_value:
                normal_value = v.get('normal-value', self.default_normal_value)
            _log.debug("{measurement_id},{normal_value}{class_name},{type},{power},{eqtype}".format(measurement_id=k,
                                                                                                    class_name=v['class'],
                                                                                                    type=v['type'],
                                                                                                    power=cn_nomv,
                                                                                                    eqtype=eqtype,
                                                                                                    normal_value=normal_value))
            if v['class'] == 'Discrete':
                self._sensors[k] = DiscreteSensor(perunit_drop_rate, agg_interval)
            else:
                self._sensors[k] = Sensor(normal_value=normal_value,
                                          aggregation_interval=agg_interval,
                                          perunit_drop_rate=perunit_drop_rate,
                                          perunit_confidence_band=perunit_confidence_rate)
        _log.info("Created {} sensors".format(len(self._sensors)))
        self._first_time_through = True
        self.sensor_file = open("/tmp/sensor.data.txt", 'w')
        self.measurement_file = open("/tmp/measurement.data.txt", 'w')
        self._simulation_complete = False
        self.measurement_in_file = open("/tmp/measurement.infile.txt", 'w')
        self.measurement_out_file = open("/tmp/measurement.outfile.txt", 'w')

    def simulation_complete(self):
        self._simulation_complete = True

    def on_simulation_message(self, headers, message):
        """
        Listen for simulation measurement messages off the gridappsd message bus.

        Each measurement is mapped onto a `Sensor` which determines whether or not the
        measurement is published to the sensor output topic or dropped.

        :param headers:
        :param message:
            Simulation measurement message.
        """
        _log.debug("Measurement Detected")
        measurement_out = {}

        if self._first_time_through:
            with open("/tmp/measurement_list.txt", 'w') as mef:
                for x in message['message']['measurements']:
                    mef.write(f'"{x}": '+'{},\n')
            self._first_time_through = False

        self.measurement_in_file.write(f"{json.dumps(message)}\n")

        # if passthrough set then copy over the measurmments of the entire message
        # into the output.
        #if self.passthrough_if_not_specified:
        measurement_out = deepcopy(message['message']['measurements'])

        timestamp = message['message']['timestamp']

        # Loop over the configured sensor andding measurements for each of them
        for mrid in self._sensors:
            new_measurement = dict(
                measurement_mrid=mrid
            )

            _log.debug(f"Getting message from sensor: {mrid}")
            item = message['message']['measurements'].get(mrid)

            if not item:
                _log.error(f"Invalid sensor mrid configured {mrid}")
                continue

            # Create new values for data from the sensor.
            for prop, value in item.items():
                if prop in ('measurement_mrid',):
                    new_measurement[prop] = value
                    continue
                new_value = None

                if prop == 'magnitude':
                    self.measurement_file.write(f"{timestamp} {mrid}, magnitude: {value}\n")
                elif prop == 'angle':
                    self.measurement_file.write(f"{timestamp} {mrid}, angle: {value}\n")

                sensor = self._sensors[mrid]

                if prop in ('angle', 'magnitude'):
                    sensor_prop = sensor.get_property_sensor(prop)
                    if sensor_prop is None:
                        # sensor is normally 0
                        sensor.add_property_sensor(prop, 180, sensor.interval, sensor.perunit_dropping,
                                                   sensor._perunit_confidence_band_95pct)
                        sensor_prop = sensor.get_property_sensor(prop)
                    new_value = sensor_prop.get_new_value(timestamp, value)
                else:
                    # Keep values other than angle and magnitued the same for now.
                    new_measurement[prop] = value

                if new_value is None:
                    new_measurement = None
                    _log.debug(f"Not reporting measurement for ts: {timestamp} {mrid}")
                    break

                _log.debug(f"mrid: {mrid} timestamp: {timestamp} prop: {prop} new_value: {new_value}")
                if prop == 'magnitude':
                    self.sensor_file.write(f"{timestamp} {mrid}, {new_value}\n")
                elif prop == 'angle':
                    self.sensor_file.write(f"{timestamp} {mrid}, {value}\n")
                new_measurement[prop] = new_value

            if new_measurement is not None:
                _log.debug(f"Adding new measurement: {new_measurement}")
                measurement_out[mrid] = new_measurement

        if len(measurement_out) > 0:
            message['message']['measurements'] = measurement_out
            if self._log_statistics:
                self._log_sensors()
            _log.info(f"Sensor Measurements:\n{measurement_out}")
            self.measurement_out_file.write(f"{json.dumps(message)}\n")
            self._gappsd.send(self._write_topic, message)
        else:
            _log.info("No sensor output.")

    def _log_sensors(self):
        for s in self._sensors:
            _log.debug(s)
            self._logger.debug(s)

    def main_loop(self):
        self._gappsd.subscribe(self._read_topic, self.on_simulation_message)

        while True and not self._simulation_complete:
            time.sleep(0.001)

        self.measurement_file.close()
        self.sensor_file.close()
        self.measurement_in_file.close()


class Sensor(object):
    def __init__(self, normal_value, aggregation_interval, perunit_drop_rate,
                 perunit_confidence_band):
        """
        An object modeling an individual sensor.

        :param normal_value: Nominal value of the quantity which the
            sensor is measuring. E.g. 120 or 240 if measuring voltage
            magnitude of a typical home in the U.S.
        :param aggregation_interval: Interval (seconds) for which
            measurements are collected before aggregation is performed.
        :param perunit_drop_rate: Number on interval [0, 1), indicating
            the chance (from uniform distribution) measurements will be
            dropped. E.g. if perunit_drop_rate = 0.1, 10% of
            measurements will be dropped over the long run.
        :param perunit_confidence_band: with a 95 % confidence interval, we are 95 % certain
            that the true value lies within an interval this wide, centered on the measured value.

        """
        self._normal_value = normal_value
        self._perunit_drop_rate = perunit_drop_rate
        self._perunit_confidence_band_95pct = perunit_confidence_band
        # 3.92 = 1.96 * 2.0
        self._stddev = normal_value * perunit_confidence_band / 3.92   # for normal two sided distribution
        self._interval = aggregation_interval
        # Set default - Uninitialized values for internal properties.
        self._n = 0
        self._tstart = 0
        self._average = 0
        self._min = 0
        self._max = 0
        self._initialized = False
        self._complex = False
        # A secondary list of sensors
        self._properties = {}

        #_log.debug(self)

    def add_property_sensor(self, key, normal_value, aggregation_interval, perunit_drop_rate,
                            perunit_confidence_band):
        if key in self._properties:
            raise KeyError(f"key {key} already exists in the sensor properties")

        self._properties[key] = Sensor(normal_value, aggregation_interval, perunit_drop_rate, perunit_confidence_band)

    def get_property_sensor(self, key):
        if key == 'magnitude':
            return self
        return self._properties.get(key)

    def __repr__(self):
        return f"""
<Sensor(nominal={self.normal_value}, interval={self.interval}, perunit_drop_rate={self.perunit_dropping}, 
    perunit_confidence_rate={self._perunit_confidence_band_95pct})>"""

    @property
    def normal_value(self):
        return self._normal_value

    @property
    def perunit_dropping(self):
        return self._perunit_drop_rate

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
        return "nominal: {}, stddev: {}, pu dropped: {}, agg interval: {}".format(
            self.normal_value, self.stddev, self.perunit_dropping, self.interval
        )


class DiscreteSensor(Sensor):

    def __init__(self, perunit_drop_rate, aggregation_interval):
        super(DiscreteSensor, self).__init__(0, aggregation_interval, perunit_drop_rate, 0)
        self._perunit_drop_rate = perunit_drop_rate
        self._value = None

    def add_sample(self, t, val):
        if not self._initialized:
            self.initialize(t, val)
        if t - self._tstart <= self._interval:
            self._n = self._n + 1
        self._value = val

    def take_inst_sample(self, t):
        if self._perunit_drop_rate > 0.0:
            drop = random.uniform(0, 1)
            if drop <= self._perunit_drop_rate:
                # reset the interval back to initial state.
                self._n = 1
                return None
        return self._value

    def get_new_value(self, t, value):
        _log.debug("Discrete get_new_value()")
        self.add_sample(t, value)
        if self.ready_to_sample(t):
            return self.take_inst_sample(t)
        return None

    def __str__(self):
        return "DiscreteSensor <perunit_drop_rate: {perunit_drop_rate}, aggregation_interval: {aggregation_interval}>" \
            .format(perunit_drop_rate=self._perunit_drop_rate, aggregation_interval=self._interval)
