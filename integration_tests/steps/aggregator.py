from behave import given, when, then


@then('the grid fees are correctly calculated')
def step_impl(context):
    initial_house2_grid_fee = 1
    updated_house2_grid_fee = context.device.updated_house2_grid_fee_cents_kwh
    initial_expected_grid_fees = {'Grid': 11, 'House 1': 13, 'House 2': 1}
    expected_grid_fees = {}
    for k, v in initial_expected_grid_fees.items():
        expected_grid_fees[k] = v - initial_house2_grid_fee + updated_house2_grid_fee

    assert context.device.initial_grid_fees_market_cycle == initial_expected_grid_fees
    assert context.device.grid_fees_market_cycle_next_market == expected_grid_fees
    assert context.device.grid_fees_tick_last_market == initial_expected_grid_fees
