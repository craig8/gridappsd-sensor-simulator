from sensors.sensor import  Sensor
from sensors import Sensors

import mock


def test_create_sensor():
    parent = mock.Mock(spec=Sensors)
    s = Sensor(parent, aggregation_interval=5, normal_value=555,
               perunit_confidence_band=0.01, perunit_drop_rate=0)

    # Expectation is that 0-3 return none values and the x == 4 value
    # returns the average and resets for the next response.
    for x in range(0, 4):
        v = s.get_new_value(x, 555)
        assert x + 1 == s._n

        assert v is None
    x = 4  # actually 5th value in the timestep
    v = s.get_new_value(x, 555)
    assert v is not None

    # the last v is now the first entry in the next sampled data
    assert 1 == s._n
    assert 4 == s._tstart
    assert 555 == s._max
    assert 555 == s._min
    assert 555 == s._average

    for x in range(5, 8):
        v = s.get_new_value(x, 555)

        assert x - 3 == s._n

        assert v is None

    x = 8
    v = s.get_new_value(x, 555)
    assert v is not None
    assert 1 == s._n
    assert 8 == s._tstart

