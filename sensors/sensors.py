from copy import deepcopy
import json
import logging
import time

from . sensor import Sensor
from .sensordao import SensorDao
from . user_config import UserConfig
_log = logging.getLogger(__name__)

_log.setLevel(logging.DEBUG)


class Sensors(object):
    def __init__(self, gridappsd, read_topic, write_topic, feeder, user_config: UserConfig,
                 logger=None, dao: SensorDao = None):
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
                    # Added by starting of sensor_simulator
                    "time_multiple": 1000
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
        global _log
        if logger:
            _log = logger
        super(Sensors, self).__init__()

        self._user_config = UserConfig()
        # if user_options is None:
        #     user_options = {}
        # else:
        #     user_options = deepcopy(user_options)
        # self._random_seed = user_options.get('random-seed', 0)
        self._sensors = {}
        self._gappsd = gridappsd
        self._logger = self._gappsd.get_logger()
        self._feeder = feeder
        self._read_topic = read_topic
        self._write_topic = write_topic
        self._log_statistics = False
        # time_multiple = user_options['time_multiple']

        assert self._gappsd, "Invalid gridappsd object specified, cannot be None"
        assert self._read_topic, "Invalid read topic specified, cannot be None"
        assert self._write_topic, "Invalid write topic specified, cannob be None"

        self._possible_measurement_sensors = {}
        self._simulation_complete = False

        self._measurement_number = 0
        self._dao = dao
        self._user_config = user_config
        # if self.simulate_all:
        #     sensors_all = get_sensors_config(feeder)
        #     sensors_config.update(sensors_all)

        # _log.debug(f"sensors_config is: {sensors_config}")
        # random.seed(self._random_seed)
    @property
    def user_config(self) -> UserConfig:
        return self._user_config

    def add_sensor(self, mesurement_mrid, normal_value, aggregation_interval=None) -> Sensor:
        if self._dao:
            self._dao.create_measurement(mesurement_mrid)

        cfg = {}
        if mesurement_mrid in self._user_config.sensors_config:
            cfg = self._user_config.sensors_config[mesurement_mrid]

        if aggregation_interval is None:
            aggregation_interval = self._user_config.aggregation_interval
        else:
            aggregation_interval = int(aggregation_interval)

        self._sensors[mesurement_mrid] = Sensor(parent=self,
                                                normal_value=cfg.get("normal-value", normal_value),
                                                aggregation_interval=cfg.get("aggregation-interval",
                                                                             aggregation_interval))
        return self._sensors[mesurement_mrid]

    def load_user_options(self, user_options: dict = None):
        self._user_config = UserConfig(user_options)

        # self._sensors = {}
        # self._initialized = False
        # self._simulation_complete = False
        # self._measurement_number = 0
        _log.debug("measurement_id,normal_value,class,type,power,eqtype")
        # for k, v in sensors_config.items():
        #     cn_nomv = ''
        #     try:
        #         has_cn_pnv = True
        #         cn_nomv = v['cn_nomv']
        #     except KeyError:
        #         has_cn_pnv = False
        #
        #     amp = ''
        #     eqtype = ''
        #     try:
        #         has_va = True
        #         amp = v['current_nomv']['val']
        #         eqtype = v['current_nomv']['eqtype']
        #         va_normal = v['current_nomv']['va_normal']
        #     except KeyError:
        #         has_va = False
        #
        #     normal_value = None
        #     type_name = None
        #     # Only if queried from db will these be valid.
        #     if 'type' in v:
        #         type_name = v['type']
        #         if v['type'] == 'VA' and has_va:
        #             normal_value = va_normal
        #         elif v['type'] == 'PNV' and has_cn_pnv:
        #             normal_value = cn_nomv
        #     agg_interval = v.get("aggregation-interval", self.default_aggregation_interval)
        #     # Make sure that the sensor has the correct scaled time value.
        #     if agg_interval != self.default_aggregation_interval:
        #         agg_interval = agg_interval * time_multiple
        #     perunit_drop_rate = v.get("perunit-drop-rate", self.default_drop_rate)
        #     perunit_confidence_rate = v.get('perunit-confidence-band', self.default_perunit_confifidence_band)
        #
        #     if not normal_value:
        #         normal_value = v.get('normal-value', self.default_normal_value)
        #     class_name = None
        #     # Only available when loading from database
        #     if 'class' in v:
        #         class_name = v['class']
        #     _log.debug("{measurement_id},{normal_value}{class_name},{type_name},{power},{eqtype}".format(measurement_id=k,
        #                                                                                                  class_name=class_name,
        #                                                                                                  type_name=type_name,
        #                                                                                                  power=cn_nomv,
        #                                                                                                  eqtype=eqtype,
        #                                                                                                  normal_value=normal_value))
        #     if class_name == 'Discrete':
        #         self._sensors[k] = DiscreteSensor(perunit_drop_rate, agg_interval)
        #     else:
        #         self._sensors[k] = Sensor(normal_value=normal_value,
        #                                   aggregation_interval=agg_interval,
        #                                   perunit_drop_rate=perunit_drop_rate,
        #                                   perunit_confidence_band=perunit_confidence_rate)
        # _log.info("Created {} sensors".format(len(self._sensors)))
        # self._first_time_through = True
        # self.sensor_file = open("/tmp/sensor.data.txt", 'w')
        # self.measurement_file = open("/tmp/measurement.data.txt", 'w')
        # self._simulation_complete = False
        # self.measurement_in_file = open("/tmp/measurement.infile.txt", 'w')
        # self.measurement_out_file = open("/tmp/measurement.outfile.txt", 'w')

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
        self._measurement_number += 1
        _log.debug(f"Measurement Detected {self._measurement_number}")
        measurement_out = {}

        current_measurements = message['message']['measurements']

        # if not self._initialized:
        #     # Build sensors for the current payload
        #     for meas_mrid, measurement in current_measurements.items():
        #         if measurement['eqtype'] == 'EnergyConsumer':
        #             self._sensors[measurement_number] = EnergyConsumer()
        #
        #
        #
        # #if self._first_time_through:
        # try:
        #     with open(f"/tmp/measurement_list{measurement_number}.txt", 'w') as mef:
        #         for x, v in message['message']['measurements'].items():
        #             mef.write(f'"{x}": '+f'{v},\n')
        #     self._first_time_through = False
        # except Exception as e:
        #     with open("/tmp/sensor_error.txt", 'w') as issue:
        #         issue.write(f"{e.args}\n")
        # self.measurement_in_file.write(f"{json.dumps(message)}\n")

        # if passthrough set then copy over the measurmments of the entire message
        # into the output.
        # if self.passthrough_if_not_specified:
        measurement_out = deepcopy(message['message']['measurements'])

        timestamp = message['message']['timestamp']

        # Loop over the configured sensor anding measurements for each of them
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

                # if prop == 'magnitude':
                #     self.measurement_file.write(f"{timestamp} {mrid}, magnitude: {value}\n")
                # elif prop == 'angle':
                #     self.measurement_file.write(f"{timestamp} {mrid}, angle: {value}\n")

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
                # if prop == 'magnitude':
                #     self.sensor_file.write(f"{timestamp} {mrid}, {new_value}\n")
                # elif prop == 'angle':
                #     self.sensor_file.write(f"{timestamp} {mrid}, {value}\n")
                if self._dao:
                    self._dao.add_to_batch(measurement_mrid=item['measurement_mrid'],
                                           sensor_prop=prop,
                                           ts=timestamp,
                                           original_value=value,
                                           sensor_value=new_value)
                new_measurement[prop] = new_value

            if new_measurement is not None:
                _log.debug(f"Adding new measurement: {new_measurement}")
                measurement_out[mrid] = new_measurement

        if len(measurement_out) > 0:
            if self._dao:
                self._dao.submit_batch()
            message['message']['measurements'] = measurement_out
            if self._log_statistics:
                self._log_sensors()
            # _log.info(f"Sensor Measurements:\n{measurement_out}")
            # self.measurement_out_file.write(f"{json.dumps(message)}\n")
            self._gappsd.send(self._write_topic, message)
        else:
            _log.info("No sensor output.")

    def _log_sensors(self):
        for s in self._sensors:
            _log.debug(s)
            self._logger.debug(s)

    def main_loop(self):
        _log.debug("Begining main loop")
        _log.info(f"Listening to {self._read_topic} for simulation messages.")
        self._gappsd.subscribe(self._read_topic, self.on_simulation_message)

        while True and not self._simulation_complete:
            time.sleep(0.001)

        # self.measurement_file.close()
        # self.sensor_file.close()
        # self.measurement_in_file.close()
