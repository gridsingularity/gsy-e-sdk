# pylint: disable=missing-function-docstring

from behave import step  # pylint: disable=no-name-in-module

from integration_tests.test_aggregator_market import MarketAggregator


@step("the gsy-e-sdk is connected {area_id} market on gsy-e")
def step_impl(context, area_id):
    context.aggregator = MarketAggregator(area_id)
    assert context.aggregator.is_active
