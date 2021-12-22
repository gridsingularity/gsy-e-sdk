# pylint: disable=missing-function-docstring
# pylint: disable=function-redefined
from math import isclose
from os import system
from time import sleep

from behave import given, when, then, step  # pylint: disable=no-name-in-module

from integration_tests.sim_container_redis_commands import send_resume_to_simulation
from integration_tests.test_aggregator_batch_commands import BatchAggregator
from integration_tests.test_aggregator_ess import EssAggregator
from integration_tests.test_aggregator_load import LoadAggregator
from integration_tests.test_aggregator_pv import PVAggregator
from integration_tests.utils import wait_for_log_to_appear_in_container_logs

REDIS_HOST = "redis.container"
REDIS_PORT = 6379


@given("redis container is started")
def step_impl(_context):
    container_name = "gsy-e-tests-redis"
    system(f"docker run -d -p {REDIS_PORT}:{REDIS_PORT} --name {container_name} -h {REDIS_HOST} "
           "--net integtestnet redis:6.2.5")
    assert wait_for_log_to_appear_in_container_logs(container_name, "Ready to accept connections")


@given("gsy-e is started in paused mode using setup {setup_file} ({gsy_e_options})")
def step_impl(_context, setup_file: str, gsy_e_options: str):
    """Run the d3a container on a specific setup.

    Args:
        setup_file (str): the setup file for a d3a simulation.
        gsy_e_options (str): options to be passed to the d3a run command. E.g.: "-t 1s -d 12h"
    """
    container_name = "gsy-e-tests"
    system(f"docker run -d --name {container_name} "
           f"--env REDIS_URL=redis://{REDIS_HOST}:{REDIS_PORT}/ "
           f"--net integtestnet gsy-e-tests -l INFO run --setup {setup_file} "
           f"--no-export --seed 0 --enable-external-connection {gsy_e_options} --paused")

    assert wait_for_log_to_appear_in_container_logs(container_name, "Simulation paused")


@step("gsy-e is resumed")
def step_impl(_context):
    send_resume_to_simulation()


@when("the gsy-e-sdk is connecting to gsy-e with test_aggregator_load")
def step_impl(context):
    context.aggregator = LoadAggregator("load")
    assert context.aggregator.is_active


@when("the gsy-e-sdk is connecting to gsy-e with test_aggregator_batch_commands")
def step_impl(context):
    context.aggregator = BatchAggregator(aggregator_name="My_aggregator")
    assert context.aggregator.is_active


@when("the gsy-e-sdk is connecting to gsy-e with test_aggregator_pv")
def step_impl(context):
    context.aggregator = PVAggregator("pv_aggregator")
    assert context.aggregator.is_active


@then("the on_event_or_response is called for different events")
def step_impl(context):
    # Check if the market event triggered both the on_market_cycle and on_event_or_response
    assert context.aggregator.events == {
        "event", "command",
        "tick", "register",
        "offer_delete", "trade",
        "offer", "unregister",
        "list_offers", "market", "finish"}
    assert context.aggregator.is_on_market_cycle_called


@when("the gsy-e-sdk is connecting to gsy-e with test_aggregator_ess")
def step_impl(context):
    context.aggregator = EssAggregator("storage_aggregator")
    assert context.aggregator.is_active


@step("the gsy-e-sdk is connected to the gsy-e until finished")
def step_impl(context):
    # Infinite loop in order to leave the client running on the background
    # placing bids and offers on every market cycle.
    # Should stop if an error occurs or if the simulation has finished
    counter = 0  # Wait for five minutes at most
    while (
            len(context.aggregator.errors) == 0
            and context.aggregator.status != "finished"
            and counter < 60):
        sleep(3)
        counter += 3


@step("the gsy-e-sdk does not report errors")
def step_impl(context):
    assert len(context.aggregator.errors) == 0, \
        f"The following errors were reported: {context.aggregator.errors}"


@then("the energy bills of the load report the required energy was bought by the load")
def step_impl(context):
    assert isclose(context.aggregator.device_bills["bought"], 22 * 0.2)
