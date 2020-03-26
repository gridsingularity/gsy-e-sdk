from pendulum import today
from behave import given, when, then
from time import sleep
from d3a_api_client.market import MarketClient
from d3a_interface.constants_limits import DATE_TIME_FORMAT


@given('the external client is started that connects to {area_id} market')
def step_impl(context, area_id):
    sleep(5)
    context.redis_market = MarketClient(area_id)


@when('the external client is requesting market stats')
def step_impl(context):
    # wait enough time to finish at least the first market slot
    sleep(5)
    context.market_slot_string_1 = today().format(DATE_TIME_FORMAT)
    context.market_slot_string_2 = today().add(minutes=60).format(DATE_TIME_FORMAT)
    context.results = context.redis_market.list_market_stats([context.market_slot_string_1,
                                                              context.market_slot_string_2])


@then('the market stats are reported correctly')
def step_impl(context):
    assert "market_fee_const" in context.results
    assert set(context.results["market_fee_const"].keys()) == {context.market_slot_string_1,
                                                               context.market_slot_string_2}
