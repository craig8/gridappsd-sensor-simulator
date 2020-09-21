from copy import deepcopy
import json
import logging
from pathlib import Path
import sys

import pytest

from sensors.measurements import Measurements
from sensors.sensor import Sensors


TEST_DATA_PATH = Path(__file__).parent.joinpath("test_data")
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
LOG = logging.getLogger()

class FakeGridAPPS():
    def __init__(self):
        self._published = []
        self._measurements_published = []
        self._timestamps_published = []
    def clear(self):
        self._published.clear()
        self._measurements_published.clear()
        self._timestamps_published.clear()
    def get_published(self):
        return self._published
    def get_timestamps_published(self):
        return self._timestamps_published
    def get_measurements_published(self):
        return self._measurements_published
    def send(self, topic, message):
        self._published.append(dict(topic=topic, message=message))
        self._measurements_published.append(message['message']['measurements'])
        self._timestamps_published.append(message['message']['timestamp'])
    def get_logger(self):
        return LOG

user_options = {
        "default-perunit-confidence-band": 0.02,
        "simulate-all": False,
        "sensors-config": {},
        "default-normal-value": 100,
        "random-seed": 2500,
        "default-aggregation-interval": 0,
        "passthrough-if-not-specified": False,
        "default-perunit-drop-rate": 0.05
    }

with open(str(TEST_DATA_PATH.joinpath('sensors_meta.json'))) as fp:
    sensors_meta = json.load(fp)

params = {
    "feeder": "_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62",
    "sensors_meta": sensors_meta
}


def get_sensors(fake_gridappsd, user_options):
    sensors = Sensors(fake_gridappsd, "read", "write", sensors_meta, user_options)
    return sensors


mfp = None
def next_measurement():
    global mfp

    if mfp is None:
        mfp = open(str(TEST_DATA_PATH.joinpath('fncs_measurements.json')))

    line = mfp.readline()
    if line:
        return json.loads(line)

    return None


def aggregate_sensor_prop(messages_in, mrid, prop):
    value = 0
    #import pdb; pdb.set_trace()
    for m in messages_in:
        value += m['message']['message']['measurements'][mrid][prop]
    return value

def test_simulate_all():
    # customize options for this test
    opts = deepcopy(user_options)
    opts["simulate-all"] = True
    gappsd = FakeGridAPPS()
    
    sensors = get_sensors(gappsd, opts)
    
    # Each measurement should have its own sensor object
    assert len(sensors_meta) == len(sensors._sensors)

    # turn off simulate-all, since we aren't having any
    # sensors put in the user_opts then we have 0 sensors 
    opts["simulate-all"] = False
    sensors = get_sensors(gappsd, opts)
    assert len(sensors._sensors) == 0


def run_simulation(opt_updates, num_lines=1):
    global mfp
    # Reset to the head of the file.
    if mfp is not None:
        mfp = None

    opts = deepcopy(user_options)
    opts.update(opt_updates)
   
    gappsd = FakeGridAPPS()
    sensors = get_sensors(gappsd, opts)

    messages_in = []
    pub_messages = []
    num_pub_messages = num_lines

    for i in range(num_pub_messages):
        pub_message = next_measurement()
        messages_in.append(pub_message)
        # copy the message to send to the message so that
        # it doesn't modify the message.
        cp = deepcopy(pub_message)
        sensors.on_simulation_message(cp['headers'], cp['message'])
   
    return gappsd, sensors, messages_in

    
def test_single_sensor_single_publish():
    # customize options for this test
    opts = deepcopy(user_options)
    opts["passthrough-if-not-specified"] = False
    opts["simulate-all"] = False
    opts["sensors-config"] = {
            "_392f7130-efe9-493c-bd42-3a31a556b20d": {}
    }
    
    # number of lines to read from the file.
    num_pub_messages = 1
    # import pdb; pdb.set_trace()
    gappsd, sensors, messages_in = run_simulation(opts, num_pub_messages)

    # since we only configure a single sensor and we have simulate-all = False
    # this will be the only output
    assert len(sensors._sensors) == 1

    measurements = gappsd.get_measurements_published()
    timestamps = gappsd.get_timestamps_published()
    # import pdb; pdb.set_trace() 
    # We should have 2 measurements, and timestamps now due to the publishing 2x
    # assert num_pub_messages - 1
    assert num_pub_messages == len(timestamps)
    assert num_pub_messages == len(measurements)
    
    for i, m in enumerate(measurements):
        for k in list(m.keys()):
            angle = m[k].get('angle')
            mag = m[k].get('magnitude')

            original_angle = messages_in[i]['message']['message']['measurements'][k].get('angle')
            original_mag = messages_in[i]['message']['message']['measurements'][k].get('magnitude')

            assert original_mag != mag, f"{original_mag} == {mag} and it shouldn't"
            assert original_angle != angle, f"{original_angle} == {angle} and it shouldn't"

def test_aggregation_non_evenly_divisible_interval():
    # customize options for this test
    opts = deepcopy(user_options)
    opts["passthrough-if-not-specified"] = False
    opts["simulate-all"] = False
    opts["sensors-config"] = {
            "_392f7130-efe9-493c-bd42-3a31a556b20d": {
                "aggregation-interval": 3
            }
    }
    
    # number of lines to read from the file.
    num_pub_messages = 5
    gappsd, sensors, messages_in = run_simulation(opts, num_pub_messages)

    # since we only configure a single sensor and we have simulate-all = False
    # this will be the only output
    assert len(sensors._sensors) == 1

    measurements = gappsd.get_measurements_published()
    timestamps = gappsd.get_timestamps_published()
    #import pdb; pdb.set_trace()   
    # in this setup we expect a single message to be published with
    # aggregation
    assert 1 == len(timestamps)
    assert 1 == len(measurements)

    for i, m in enumerate(measurements):
        # Loop over each of the measurements which means that k is the
        # mrid for each of the measurements.
        for k in list(m.keys()):
            angle_agg = aggregate_sensor_prop(messages_in, k, 'angle')   
            mag_agg = aggregate_sensor_prop(messages_in, k, 'magnitude')
            angle = m[k].get('angle')
            mag = m[k].get('magnitude')

            # compare a straight aggregation with what is output by the
            # sensor service.
            assert angle_agg != angle
            assert mag_agg != mag
            print(f"angle={angle}, mag={mag}")


def test_aggregation_publish():
    # customize options for this test
    opts = deepcopy(user_options)
    opts["passthrough-if-not-specified"] = False
    opts["simulate-all"] = False
    opts["sensors-config"] = {
            "_392f7130-efe9-493c-bd42-3a31a556b20d": {
                "aggregation-interval": 3
            }
    }
    
    # number of lines to read from the file.
    num_pub_messages = 3
    gappsd, sensors, messages_in = run_simulation(opts, num_pub_messages)

    # since we only configure a single sensor and we have simulate-all = False
    # this will be the only output
    assert len(sensors._sensors) == 1

    measurements = gappsd.get_measurements_published()
    timestamps = gappsd.get_timestamps_published()
   
    # in this setup we expect a single message to be published with
    # aggregation
    assert 1 == len(timestamps)
    assert 1 == len(measurements)

    for i, m in enumerate(measurements):
        # Loop over each of the measurements which means that k is the
        # mrid for each of the measurements.
        for k in list(m.keys()):
            angle_agg = aggregate_sensor_prop(messages_in, k, 'angle')   
            mag_agg = aggregate_sensor_prop(messages_in, k, 'magnitude')
            angle = m[k].get('angle')
            mag = m[k].get('magnitude')

            # compare a straight aggregation with what is output by the
            # sensor service.
            assert angle_agg != angle
            assert mag_agg != mag
            print(f"angle={angle}, mag={mag}")

