# pylint: disable=missing-function-docstring
from time import sleep

from behave import step  # pylint: disable=no-name-in-module

from integration_tests.test_aggregator_market import MarketAggregator


@step("the gsy-e-sdk is connected {area_id} market on gsy-e")
def step_impl(context, area_id):
    context.aggregator = MarketAggregator(area_id)
    sleep(3)
    assert context.aggregator.is_active is True
