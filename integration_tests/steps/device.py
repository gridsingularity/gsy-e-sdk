from math import isclose
from os import system
from time import sleep

from behave import given, when, then

from integration_tests.test_aggregator_batch_commands import BatchAggregator
from integration_tests.test_aggregator_ess import EssAggregator
from integration_tests.test_aggregator_load import LoadAggregator
from integration_tests.test_aggregator_pv import PVAggregator


@given("redis container is started")
def step_impl(context):
    system(f"docker run -d -p 6379:6379 --name redis.container -h redis.container "
           "--net integtestnet gsyd3a/d3a:redis-staging")


@given("d3a is started using setup {setup_file} ({d3a_options})")
def step_impl(context, setup_file: str, d3a_options: str):
    """Run the d3a container on a specific setup.

    Args:
        setup_file (str): the setup file for a d3a simulation.
        d3a_options (str): options to be passed to the d3a run command. E.g.: "-t 1s -d 12h"
    """
    sleep(3)
    system(f"docker run -d --name d3a-tests --env REDIS_URL=redis://redis.container:6379/ "
           f"--net integtestnet d3a-tests -l INFO run --setup {setup_file} "
           f"--no-export --seed 0 --enable-external-connection {d3a_options} ")


@when("the external client is started with test_aggregator_load")
def step_impl(context):
    # Wait for d3a to activate all areas
    sleep(5)
    # Connects one client to the load device
    context.aggregator = LoadAggregator("load")
    sleep(3)
    assert context.aggregator.is_active is True


@when("the external client is started with test_aggregator_batch_commands")
def step_impl(context):
    # Wait for d3a to activate all areas
    sleep(5)
    # Connects one client to the load device
    context.aggregator = BatchAggregator(aggregator_name="My_aggregator")
    sleep(3)
    assert context.aggregator.is_active is True


@when("the external client is started with test_aggregator_pv")
def step_impl(context):
    # Wait for d3a to activate all areas
    sleep(5)
    # Connects one client to the load device
    context.aggregator = PVAggregator("pv_aggregator")
    sleep(3)
    assert context.aggregator.is_active is True


@then("the on_event_or_response is called for different events")
def step_impl(context):
    # Check if the market event triggered both the on_market_cycle and on_event_or_response
    assert context.aggregator.events == {"event", "command",
                                     "tick", "register",
                                     "offer_delete", "trade",
                                     "offer", "unregister",
                                     "list_offers", "market", "finish"}
    assert context.aggregator.is_on_market_cycle_called


@when("the external client is started with test_aggregator_ess")
def step_impl(context):
    # Wait for d3a to activate all areas
    sleep(5)
    # Connects one client to the load device
    context.aggregator = EssAggregator("storage_aggregator")


@then("the external client is connecting to the simulation until finished")
def step_impl(context):
    # Infinite loop in order to leave the client running on the background
    # placing bids and offers on every market cycle.
    # Should stop if an error occurs or if the simulation has finished
    counter = 0  # Wait for five minutes at most
    while context.aggregator.errors == 0 and context.aggregator.status != "finished" and counter < 60:
        sleep(3)
        counter += 3


@then("the external client does not report errors")
def step_impl(context):
    assert context.aggregator.errors == 0


@then("the energy bills of the load report the required energy was bought by the load")
def step_impl(context):
    assert isclose(context.aggregator.device_bills["bought"], 22 * 0.2)

