from copy import deepcopy


class SensorConfig(object):
    def __init__(self, aggregation_interval, confidence_band, drop_rate):
        self.aggregation_interval = aggregation_interval
        self.confidence_band = confidence_band
        self.drop_rate = drop_rate


class UserConfig(object):

    def __init__(self, user_options: dict = None):
        if user_options is None:
            user_options = {}
        self._perunit_confidence_band = user_options.get("default-perunit-confidence-band", 0.02)
        self._aggregation_interval = user_options.get("default-aggregation-interval", 30)
        self._perunit_drop_rate = user_options.get("default-perunit-drop-rate", 0.01)
        self._normal_value = user_options.get("default-normal-value", 120)

        self._sensors_config = user_options.get("sensors-config", {})
        self._pass_through_unspecified = user_options.get('passthrough-if-not-specified', False)
        self._simulate_all = user_options.get('simulate-all', False)

    @property
    def simulate_all(self):
        return self.simulate_all

    @property
    def pass_through_unspecified(self):
        return self._pass_through_unspecified

    @property
    def perunit_confidence_band(self):
        return self._perunit_confidence_band

    @property
    def aggregation_interval(self):
        return self._aggregation_interval

    @property
    def perunit_drop_rate(self):
        return self._perunit_drop_rate

    @property
    def normal_value(self):
        return self._normal_value

    @property
    def sensors_config(self):
        return deepcopy(self._sensors_config)

