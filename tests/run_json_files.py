from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
from pprint import pprint
import time

import docker
from yaml import safe_load

from gridappsd import GridAPPSD, topics as t
from gridappsd.simulation import Simulation
from gridappsd.docker_handler import (run_containers,
                                      run_dependency_containers,
                                      DEFAULT_GRIDAPPSD_DOCKER_CONFIG)

DEFAULT_SIMULATION = {
    "power_system_config": {
        "GeographicalRegion_name": "_73C512BD-7249-4F50-50DA-D93849B89C43",
        "SubGeographicalRegion_name": "_A1170111-942A-6ABD-D325-C64886DC4D7D",
        "Line_name": "_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62"
    },
    "application_config": {
        "applications": []
    },
    "simulation_config": {
        "start_time": "1358121600",
        "duration": "120",
        "simulator": "GridLAB-D",
        "timestep_frequency": "1000",
        "timestep_increment": "1000",
        "run_realtime": False,
        "simulation_name": "ieee8500",
        "power_flow_solver_method": "NR",
        "model_creation_config": {
            "load_scaling_factor": "1",
            "schedule_name": "ieeezipload",
            "z_fraction": "0",
            "i_fraction": "1",
            "p_fraction": "0",
            "randomize_zipload_fractions": False,
            "use_houses": False
        }
    },
    "test_config": {
        "events": [],
        "appId": ""
    }
}

GRIDAPPSD_SERVICE_ID = "gridappsd-sensor-simulator"


def merge_with_simulation_config(service_file):
    cp = deepcopy(DEFAULT_SIMULATION)
    with open(service_file) as fp:
        service = safe_load(fp)
        service_configs = dict(service_configs=[])
        service_configs['service_configs'].append(service)
        cp.update(service_configs)
    return cp


# Assumes the current file is in the tests directory under the directory
# that should be mounted in the gridappsd container
LOCAL_MOUNT_POINT_FOR_SERVICE = Path(__file__).parent.parent.absolute()

# Mount point inside the gridappsd container itself.  This allows the container
# to start up the services inside the container.
SERVICE_MOUNT_POINT = "/gridappsd/services/gridappsd-sensor-simulator"
CONFIG_MOUNT_POINT = "/gridappsd/services/sensor_simulator.config"

# Directory that holds our json configurations that are going to be used
TEST_DATA_DIR = Path(__file__).parent.joinpath('test_data')
# Appending data_tests elements to TEST_DATA_DIR will get full path of the
# file to be loaded for a test.
data_tests = ("test1.json",)
client = docker.from_env()

if not client.containers.list(filters=dict(name='mysql')):
    with run_dependency_containers():
        pass

container = client.containers.list(filters=dict(name='gridappsd'))

if container:
    # Stop the gridappsd container
    container[0].stop()

config = deepcopy(DEFAULT_GRIDAPPSD_DOCKER_CONFIG)

config['gridappsd']['volumes'][str(LOCAL_MOUNT_POINT_FOR_SERVICE)] = dict(
    bind=str(SERVICE_MOUNT_POINT),
    mode="rw")

fh_sim_measurement = open("measurement.txt", 'w')
fh_sensor_measurement = open("sensor.txt", 'w')


def onstart(simulation: Simulation):
    print(f"Started: {simulation.simulation_id}")


def onfinish(simulation: Simulation):
    print(f"Finished: {simulation.simulation_id}")


def ontimestep(simulation: Simulation, timestep):
    print(f"ontimestep {timestep}")


def onmeasurement(simulation: Simulation, timestep, measurements):
    print(f"Measurement {timestep}")
    fh_sim_measurement.write(f"{json.dumps(measurements)}\n")
    #pprint(measurements)


def onsensoroutput(headers, message):
    ts = headers['timestamp'] # int(int(headers['timestamp']) / 10e6)
    print(f"Sensor timestep: {ts}")
    fh_sensor_measurement.write(f"{json.dumps(message['message']['measurements'])}\n")
    # pprint(headers)
    # pprint(message)


for cfile in data_tests:
    config_file = str(TEST_DATA_DIR.joinpath(cfile))
    run_config = merge_with_simulation_config(config_file)

    # from pprint import pprint
    # pprint(config['gridappsd'])
    with run_containers(config, stop_after=True) as containers:
        # Watches the log on the container for the MYSQL data.
        containers.wait_for_log_pattern("gridappsd", "MYSQL")

        gappsd = GridAPPSD()
        gappsd.connect()
        assert gappsd.connected

        time.sleep(10)
        sim = Simulation(gappsd, run_config=run_config)

        sim.add_onstart_callback(onstart)
        sim.add_oncomplete_callback(onfinish)
        sim.add_ontimestep_callback(ontimestep)
        sim.add_onmesurement_callback(onmeasurement)

        sim.start_simulation()
        sim.pause()
        gappsd.subscribe(t.application_output_topic(GRIDAPPSD_SERVICE_ID, sim.simulation_id), onsensoroutput)
        sim.resume()
        sim.run_loop()
        print("Shutting down")

    fh_sim_measurement.close()
    fh_sensor_measurement.close()
