from time import sleep

from behave import when

from integration_tests.test_grid_fee import AutoGridFeeUpdateOnMarket


@when('DSO started the external client that connects to {area_id} market')
def step_impl(context, area_id):
    sleep(5)
    context.device = AutoGridFeeUpdateOnMarket(area_id)
