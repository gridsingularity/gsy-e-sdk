from os import system


TEST_CONTAINER_IDS = "docker ps -a -f name=gsy-e-tests* -q"
STOP_AND_REMOVE_TEST_CONTAINERS = (
    f"docker stop $({TEST_CONTAINER_IDS}) && docker rm $({TEST_CONTAINER_IDS})")


def before_all(_context):
    """Run commands before all integration tests start."""
    system(STOP_AND_REMOVE_TEST_CONTAINERS)
    system("docker network create integtestnet")
    system("bash integration_tests/build_test_containers.sh")


def after_all(_context):
    """Run commands after all integration tests."""
    system("docker network rm integtestnet")


def after_scenario(_context, _scenario):
    """Run commands before each scenario."""
    system(STOP_AND_REMOVE_TEST_CONTAINERS)
