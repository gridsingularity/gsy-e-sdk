from time import sleep

from behave import given, when

from integration_tests.test_grid_fee import AutoGridFeeUpdateOnMarket


@when('the external client requests DSO market stats')
def step_impl(context):
    time_out = 5
    sleep(5)

    while context.device.wait_script and time_out < 180:
        try:
            last_market_dso_stats = context.device.last_market_dso_stats()
            assert set(last_market_dso_stats.keys()) == \
                   {'status', 'name', 'area_uuid', 'command', 'market_stats', 'transaction_id'}

            sleep(2)
            time_out += 2
        except Exception as e:
            if context.device.wait_script:
                context.device.errors += 1
                raise e


@given('DSO started the external client that connects to {area_id} market')
def step_impl(context, area_id):
    sleep(5)
    context.device = AutoGridFeeUpdateOnMarket(area_id)

