import unittest
import os
from sensors import sensor

# Define directory
THIS_DIR = os.path.dirname(os.path.abspath(__file__))

# Get path to testing files.
DATA_DIR = os.path.join(THIS_DIR, 'data')
MEAS_FILE = os.path.join(DATA_DIR, 'simulation_measurements_13.json')
HEADER_FILE = os.path.join(DATA_DIR, 'simulation_measurements_header_13.json')


class SensorTestCase(unittest.TestCase):
    """Test the Sensor class."""

    def test_something(self):
        # Create a Sensor object. Use a nominal_voltage of 100 for nice
        # round numbers.
        s = sensor.Sensor(random_seed=42, nominal_voltage=100,
                          aggregation_interval=60)
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()
