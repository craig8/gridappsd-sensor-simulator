from pprint import pprint
import pytest
import time

from sensors.measurements import Measurements


FEEDERS = {
    "ieee123": "_C1C3E687-6FFD-C753-582B-632A27E28507",
    "ieee123pv": "_E407CBB6-8C8D-9BC9-589C-AB83FBF0826D",
    "ieee13nodeckt": "_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62",
    "ieee13nodecktassets": "_5B816B93-7A5F-B64C-8460-47C17D6E4B0F",
    "ieee8500": "_4F76A5F9-271D-9EB8-5E31-AA362D86F2C3",
    "j1": "_67AB291F-DCCD-31B7-B499-338206B9828F",
    "ieee123transactive": "_503D6E20-F499-4CC7-8051-971E23D0BF79",
    "test9500new": "_AAE94E4A-2465-6F5E-37B1-3E72183A4E44",
    "acep_psil": "_77966920-E1EC-EE8A-23EE-4EFD23B205BD",
    "sourceckt": "_9CE150A8-8CC5-A0F9-B67E-BBD8C79D3095"
}


@pytest.mark.parametrize(
    "feeder", [{k: v} for k, v in FEEDERS.items()]
)
def test_can_query_measurements(docker_dependencies, feeder):
    name = list(feeder.keys())[0]
    feeder_id = list(feeder.values())[0]
    print(name, feeder_id)

    m = Measurements()
    start_time = time.time()
    meausurements = m.get_measurements(feeder_id)
    end_time = time.time()
    print(f"Time to retrieve measurements {end_time - start_time}")
    assert len(meausurements) > 0


@pytest.mark.parametrize(
    "feeder", [{k: v} for k, v in FEEDERS.items()]
)
def test_can_get_sensors_config(docker_dependencies, feeder):
    name = list(feeder.keys())[0]
    feeder_id = list(feeder.values())[0]
    print(name, feeder_id)

    m = Measurements()
    start_time = time.time()
    meausurements = m.get_sensors_config(feeder_id)
    end_time = time.time()
    print(f"Time to retrieve configs {end_time - start_time}")
    # pprint(meausurements)
    assert len(meausurements) > 0


