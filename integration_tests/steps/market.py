from time import sleep

from behave import given

from integration_tests.test_aggregator_market import MarketAggregator


@given('the external client is started that connects to {area_id} market')
def step_impl(context, area_id):
    sleep(5)
    context.aggregator = MarketAggregator(area_id)

