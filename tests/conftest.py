from copy import deepcopy
from pathlib import Path

import pytest
from gridappsd import GOSS, GridAPPSD
from gridappsd.docker_handler import (run_dependency_containers, run_gridappsd_container, Containers,
                                      run_containers, DEFAULT_GRIDAPPSD_DOCKER_CONFIG)

# Assumes tests directory is within the directory that should be mounted to the
# gridappsd container
LOCAL_MOUNT_POINT_FOR_SERVICE = Path(__file__).parent.parent.absolute()

# Mount point inside the gridappsd container itself.
SERVICE_MOUNT_POINT = "/gridappsd/services/gridappsd-sensor-simulator"
CONFIG_MOUNT_POINT = "/gridappsd/services/sensor_simulator.config"

# If set to False then None of the containers will clean up after themselves.
# If more than one test is ran then this will cause an error because the gridappsd
# container will not be cleansed.
STOP_AFTER_FIXTURE = False


@pytest.fixture(scope="module")
def docker_dependencies():
    print("Docker dependencies")
    Containers.reset_all_containers()

    with run_dependency_containers(stop_after=STOP_AFTER_FIXTURE) as dep:
        yield dep
    print("Cleanup docker dependencies")


@pytest.fixture
def goss_client(docker_dependencies):
    with run_gridappsd_container(STOP_AFTER_FIXTURE):
        goss = GOSS()
        goss.connect()
        assert goss.connected

        yield goss

        goss.disconnect()


@pytest.fixture(scope="module")
def gridappsd_client_include_as_service_no_cleanup(docker_dependencies):

    config = deepcopy(DEFAULT_GRIDAPPSD_DOCKER_CONFIG)

    config['gridappsd']['volumes'][str(LOCAL_MOUNT_POINT_FOR_SERVICE)] = dict(
        bind=str(SERVICE_MOUNT_POINT),
        mode="rw")

    # from pprint import pprint
    # pprint(config['gridappsd'])
    with run_containers(config, stop_after=False) as containers:
        containers.wait_for_log_pattern("gridappsd", "MYSQL")

        gappsd = GridAPPSD()
        gappsd.connect()
        assert gappsd.connected

        yield gappsd

        gappsd.disconnect()


@pytest.fixture
def gridappsd_client_include_as_service(docker_dependencies):

    config = deepcopy(DEFAULT_GRIDAPPSD_DOCKER_CONFIG)

    config['gridappsd']['volumes'][str(LOCAL_MOUNT_POINT_FOR_SERVICE)] = dict(
        bind=str(SERVICE_MOUNT_POINT),
        mode="rw")

    local_config = LOCAL_MOUNT_POINT_FOR_SERVICE.joinpath("sensor_simulator.config")
    config['gridappsd']['volumes'][str(local_config)] = dict(
        bind=str(CONFIG_MOUNT_POINT),
        mode="rw")

    # from pprint import pprint
    # pprint(config['gridappsd'])
    with run_containers(config, stop_after=STOP_AFTER_FIXTURE) as containers:
        containers.wait_for_log_pattern("gridappsd", "MYSQL")

        gappsd = GridAPPSD()
        gappsd.connect()
        assert gappsd.connected

        yield gappsd

        gappsd.disconnect()
