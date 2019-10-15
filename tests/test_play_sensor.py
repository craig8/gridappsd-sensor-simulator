from typing import List

from sensors import Sensors
from copy import deepcopy
import json
import os

data_file = os.path.join(os.path.dirname(__file__), "measurment-13-node-120s.json")


def run_sensors(gapps, user_options):
    """
    Utility method to publish to the gridappsdmock instead of going through
    the gridappsd service process.

    :param gapps:
    :param user_options:
    """
    sensors = Sensors(gapps, "read", "write", user_options)
    for data in next_line():
        sensors.on_simulation_message({}, deepcopy(data))


def next_line():
    """
    Generator method that is used to read the datafile

    :return: the next line as string
    """
    with open(data_file) as fp:
        for line in fp:
            yield json.loads(line)


def build_measurement_mrids() -> List:
    """
    Build a list of measurements that are sent to the simulator service

    :return:
    """
    with open(data_file) as fp:
        data = json.loads(fp.readline())

    mrids = []
    for mrid in data['message']['measurements'].keys():
        mrids.append(mrid)

    return mrids


class GridAPPSDMock:
    """
    A mock class to allow a publisher to send messages and archive
    for later interrogation of the messages.
    """
    def __init__(self):
        self._sent_data = []
        self._logger = None

    def send(self, topic, message):
        self._sent_data.append((topic, message))

    def get_logger(self):
        return self._logger

    def get_last_received(self) -> tuple:
        if len(self._sent_data) > 0:
            return self._sent_data[-1]
        return None, None

    @property
    def sent_data(self):
        return deepcopy(self._sent_data)

    def __ne__(self, other):
        value = self.__eq__(other)
        if value is not NotImplemented:
            return not value
        return NotImplemented

    def __eq__(self, other):
        """
        Equality means that all of the elements that were received
        are of equal value and occurred in the same order.

        :type other: GridAPPSDMock
        """
        if not isinstance(other, GridAPPSDMock):
            return NotImplemented

        index = 0
        for x in self._sent_data:
            if x != other._sent_data[index]:
                return False
            index += 1

        return True


def test_random_seed():
    """
    Test that random seed produces different values for different seeds
    and same value for same seed.
    """
    gapps = GridAPPSDMock()
    measurements = build_measurement_mrids()[:2]
    sensors = {}
    for x in measurements:
        sensors[x] = {}

    user_options = {
        "sensors-config": sensors,
        "random-seed": 500
    }

    run_sensors(gapps, user_options)

    gapps2 = GridAPPSDMock()
    run_sensors(gapps2, user_options)

    assert gapps == gapps2

    gapps3 = GridAPPSDMock()
    user_options['random-seed'] = 300
    run_sensors(gapps3, user_options)
    assert gapps != gapps3
