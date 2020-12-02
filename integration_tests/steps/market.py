from pendulum import today
from behave import given, when, then
from time import sleep
from integration_tests.test_market_connection import AutoLastMarketStats
from d3a_interface.constants_limits import DATE_TIME_FORMAT
from integration_tests.test_grid_fee import AutoGridFeeUpdateOnMarket


@given('the external client is started that connects to {area_id} market')
def step_impl(context, area_id):
    sleep(5)
    context.device = AutoLastMarketStats(area_id)


@when('the external client is requesting market stats')
def step_impl(context):
    time_out = 5
    sleep(5)
    while context.device.wait_script and time_out < 180:
        try:
            last_market_stats = context.device.last_market_stats()
            assert list(last_market_stats.keys()) == \
                   ['status', 'area_uuid', 'command', 'market_stats', 'transaction_id']
            sleep(2)
            time_out += 2
        except AssertionError as e:
            sleep(2)
            if context.device.wait_script:
                context.device.errors += 1
                raise e


@then('the market stats are reported correctly')
def step_impl(context):
    assert "market_stats" in context.list_market_stats_results
    assert set(context.list_market_stats_results["market_stats"].keys()) == \
           {context.market_slot_string_1, context.market_slot_string_2}

    assert "market_stats" in context.list_dso_market_stats_results
    assert set(context.list_dso_market_stats_results["market_stats"].keys()) == \
           {context.market_slot_string_1, context.market_slot_string_2}


@when('DSO started the external client that connects to {area_id} market')
def step_impl(context, area_id):
    sleep(5)
    context.device = AutoGridFeeUpdateOnMarket(area_id)

