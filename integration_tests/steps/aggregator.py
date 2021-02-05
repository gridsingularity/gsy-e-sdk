from behave import given, when, then


@then('the grid fees are correctly calculated')
def step_impl(context):
    expected_grid_fees = {'Grid': 11, 'House 1': 13, 'House 2': 1}
    assert context.device.grid_fees_market_cycle == expected_grid_fees
    assert context.device.grid_fees_tick == expected_grid_fees
