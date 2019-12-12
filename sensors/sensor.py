from copy import deepcopy
import json
import logging
import math
import sys
import random
import time
from enum import Enum

from .measurements import get_sensors_config

_log = logging.getLogger(__name__)

class SensorType(Enum):
    pass


class Sensor(object):
    def __init__(self, parent, normal_value: float = None, aggregation_interval: int = None,
                 perunit_drop_rate: float = None, perunit_confidence_band: float = None):
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
        parent_type = type(parent)
        assert parent is not None and parent_type != "<class 'Sensors'>", \
            f"argument parent cannot be None and must be Sensors type but was {parent_type}."
        self._parent = parent
        self._normal_value = normal_value if normal_value is not None else parent.user_config.normal_value
        self._perunit_drop_rate = perunit_drop_rate if perunit_drop_rate is not None \
            else parent.user_config.perunit_drop_rate
        self._perunit_confidence_band_95pct = perunit_confidence_band if perunit_confidence_band is not None \
            else parent.user_config.perunit_confidence_band
        # 3.92 = 1.96 * 2.0
        self._stddev = self._normal_value * self._perunit_confidence_band_95pct / 3.92   # for normal two sided distribution
        self._interval = aggregation_interval if aggregation_interval is not None \
            else parent.user_config.aggregation_interval
        # Do this so that we don't have to worry about the fact that gridappsd reports every 3 seconds.
        # self._interval = math.floor(self._interval / 3.0)

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
        self._offset = 0
        #self.tag = tag
        #self.type = type
        #self.property = property

        #_log.debug(self)

    def add_property_sensor(self, key, normal_value):
        if key in self._properties:
            raise KeyError(f"key {key} already exists in the sensor properties")

        self._properties[key] = Sensor(parent=self._parent,
                                       normal_value=normal_value)

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

    @property
    def offset(self):
        return self._offset

    @property
    def perunit_confidence_band(self):
        return self._perunit_confidence_band_95pct

    def initialize(self, t, val):
        # if self._interval > 0.0:
        #     # offset = random.randint(0, self._interval - 1)  # each sensor needs a staggered start
        #     offset = self._interval  # This is temporary offset is always set.
        #     self.reset_interval(t - offset, val)
        # else:
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
        # if not initialized then add the first sample and continue.
        if not self._initialized:
            self.initialize(t, val)
            return

        if t - self._tstart < self._interval:
            if val < self._min:
                self._min = val
            if val > self._max:
                self._max = val
            self._average = self._average + val
            self._n = self._n + 1

    def ready_to_sample(self, t):
        """
        Determines if the current timestep shoudl return a sample or not.

        Example single second timestep:

            t: 0, 1, 2, 3, 4

            if interval is 5 then on the t == 4 timestep this function would return
            True

        Another example multi-second timestep:

            t: 0, 3, 6

            if the interval is 5 then on the t == 6 timestep this function would return
            True

        @param t: timestep
        """
        if t >= self._tstart + self._interval - 1:
            _log.debug("ready_to_sample(True)")
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
        return "nominal: {}, stddev: {}, pu dropped: {}, agg interval: {}, n: {}, avg: {}, min: {}, max: {}".format(
            self.normal_value, self.stddev, self.perunit_dropping, self.interval, self._n, self._average, self._min, self._max
        )

#
# class EnergyConsumer(_Sensor):
#     def __init__(self, parent, p, q):
#         self._p = p
#         self._q = q
#         self.magnitude
    # def __init__(self, perunit_drop_rate, aggregation_interval, p, q):
    #     super(EnergyConsumer, self).__init__()

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
