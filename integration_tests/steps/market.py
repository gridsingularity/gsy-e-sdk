# pylint: disable=missing-function-docstring

from behave import step  # pylint: disable=no-name-in-module

from device import wait_for_active_aggregator
from integration_tests.test_aggregator_market import MarketAggregator


@step("the gsy-e-sdk is connected {area_id} market on gsy-e")
def step_impl(context, area_id):
    context.aggregator = MarketAggregator(area_id)
    wait_for_active_aggregator(context, time_out=3)
