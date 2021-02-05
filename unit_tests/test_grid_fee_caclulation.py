import unittest
from math import isclose
from parameterized import parameterized

from d3a_api_client.grid_fee_calculation import GridFeeCalculation


grid_stats_example = \
    {'Grid 10': {'last_market_slot': '2021-02-02T00:45', 'last_market_bill': {}, 'last_market_stats': {},
               'last_market_fee': 10.0, 'next_market_fee': 11.0,
               'children': {
                   'Market Maker': {},
                   'Street 3': {'last_market_slot': '2021-02-02T00:45', 'last_market_bill': {}, 'last_market_stats': {},
                                'last_market_fee': 3.0, 'next_market_fee': 4.0,
                                'children': {
                                    'Load 3': {},
                                    'PV 3': {}}
                                },
                   'Street 1': {'last_market_slot': '2021-02-02T00:45', 'last_market_bill': {},  'last_market_stats': {},
                                'last_market_fee': 1.0, 'next_market_fee': 2.0,
                                'children': {
                                    'House 1.1': {'last_market_slot': '2021-02-02T00:45', 'last_market_bill': {}, 'last_market_stats': {},
                                                  'last_market_fee': 1.1, 'next_market_fee': 2.1,
                                                  'children': {
                                                      'Load 1.1': {},
                                                      'Storage 1.1': {}}
                                                  },
                                    'House 1.2': {'last_market_slot': '2021-02-02T00:45', 'last_market_bill': {}, 'last_market_stats': {},
                                                  'last_market_fee': 1.2, 'next_market_fee': 2.2,
                                                  'children': {
                                                      'Load 1.2': {},
                                                      'PV 1.2': {}}
                                                  }
                                }},
                   'Street 2': {'last_market_slot': '2021-02-02T00:45', 'last_market_bill': {}, 'last_market_stats': {},
                                'last_market_fee': 2.0, 'next_market_fee': 3.0,
                                'children': {
                                    'House 2.1': {'last_market_slot': '2021-02-02T00:45', 'last_market_bill': {}, 'last_market_stats': {},
                                                  'last_market_fee': 2.1, 'next_market_fee': 3.1,
                                                  'children': {
                                                      'PV 2.1': {},
                                                      'Load 2.1': {}}
                                                  }
                                }}
               }}
      }


class TestGridFeeCalculation(unittest.TestCase):

    def setUp(self):
        self.grid_fee_calc = GridFeeCalculation()
        self.grid_fee_calc.latest_grid_stats_tree = grid_stats_example
        self.grid_fee_calc._populate_grid_fee_mappings()

    @parameterized.expand([['last_market_fee'], ['next_market_fee']])
    def test_grid_fee_is_calculated_correctly_for_leaf_devices(self, fee_type):
        leaf_names = ['PV 2.1', 'PV 1.2', 'PV 3']
        for leaf_name in leaf_names:
            expected_fee = float(leaf_name.split(' ')[-1])
            if fee_type == 'next_market_fee':
                expected_fee += 1.
            assert expected_fee == self.grid_fee_calc.calculate_grid_fee(leaf_name, fee_type=fee_type)

    @parameterized.expand([['last_market_fee'], ['next_market_fee']])
    def test_grid_fee_is_calculated_correctly_for_markets(self, fee_type):
        market_names = ['House 2.1', 'House 1.2', 'Street 1', 'Grid 10']
        for market_name in market_names:
            expected_fee = float(market_name.split(' ')[-1])
            if fee_type == 'next_market_fee':
                expected_fee += 1.
            assert expected_fee == self.grid_fee_calc.calculate_grid_fee(market_name, fee_type=fee_type)

    @parameterized.expand([['last_market_fee'], ['next_market_fee']])
    def test_grid_fee_is_calculated_correctly_for_leaf_to_grid(self, fee_type):
        target_market = 'Grid 10'
        if fee_type == 'next_market_fee':
            leaf_name_expected_fee = {'PV 2.1': 17.1,
                                      'PV 1.2': 15.2,
                                      'PV 3': 15.}
        else:
            leaf_name_expected_fee = {'PV 2.1': 14.1,
                                      'PV 1.2': 12.2,
                                      'PV 3': 13.}
        for leaf_name, expected_fee in leaf_name_expected_fee.items():
            assert expected_fee == self.grid_fee_calc.calculate_grid_fee(
                start_market_or_device_name=leaf_name,
                target_market_or_device_name=target_market,
                fee_type=fee_type)

    @parameterized.expand([['last_market_fee'], ['next_market_fee']])
    def test_grid_fee_is_calculated_correctly_for_leaf_to_leaf(self, fee_type):
        if fee_type == 'next_market_fee':
            leaf_name_expected_fee = {21.3: ['PV 2.1', 'Load 1.2'],
                                      21.1: ['PV 2.1', 'Load 3'],
                                      15.0: ['Market Maker', 'Load 3']}
        else:
            leaf_name_expected_fee = {16.3: ['PV 2.1', 'Load 1.2'],
                                      17.1: ['PV 2.1', 'Load 3'],
                                      13.0: ['Market Maker', 'Load 3']}
        for expected_fee, leaf_names in leaf_name_expected_fee.items():
            assert isclose(expected_fee, self.grid_fee_calc.calculate_grid_fee(
                start_market_or_device_name=leaf_names[0],
                target_market_or_device_name=leaf_names[1],
                fee_type=fee_type))

    @parameterized.expand([['last_market_fee'], ['next_market_fee']])
    def test_grid_fee_is_calculated_correctly_for_market_to_leaf(self, fee_type):
        if fee_type == 'next_market_fee':
            leaf_name_expected_fee = {21.3: ['House 2.1', 'Load 1.2'],
                                      21.1: ['House 2.1', 'Load 3'],
                                      17.0: ['Street 1', 'Load 3']}
        else:
            leaf_name_expected_fee = {16.3: ['House 2.1', 'Load 1.2'],
                                      17.1: ['House 2.1', 'Load 3'],
                                      14.0: ['Street 1', 'Load 3']}
        for expected_fee, leaf_names in leaf_name_expected_fee.items():
            assert isclose(expected_fee, self.grid_fee_calc.calculate_grid_fee(
                start_market_or_device_name=leaf_names[0],
                target_market_or_device_name=leaf_names[1],
                fee_type=fee_type))

    @parameterized.expand([['last_market_fee'], ['next_market_fee']])
    def test_grid_fee_is_calculated_correctly_for_market_to_market(self, fee_type):
        if fee_type == 'next_market_fee':
            leaf_name_expected_fee = {21.3: ['House 2.1', 'House 1.2'],
                                      17.1: ['House 2.1', 'Grid 10'],
                                      19.1: ['Street 1', 'House 2.1']}
        else:
            leaf_name_expected_fee = {16.3: ['House 2.1', 'House 1.2'],
                                      14.1: ['House 2.1', 'Grid 10'],
                                      15.1: ['Street 1', 'House 2.1']}
        for expected_fee, leaf_names in leaf_name_expected_fee.items():
            assert isclose(expected_fee, self.grid_fee_calc.calculate_grid_fee(
                start_market_or_device_name=leaf_names[0],
                target_market_or_device_name=leaf_names[1],
                fee_type=fee_type))

    @parameterized.expand([['last_market_fee'], ['next_market_fee']])
    def test_grid_fee_is_calculated_correctly_for_device_to_parent(self, fee_type):
        if fee_type == 'next_market_fee':
            leaf_name_expected_fee = {3.1: ['House 2.1', 'Load 2.1'],
                                      2.2: ['Load 1.2', 'House 1.2', ]}
        else:
            leaf_name_expected_fee = {2.1: ['House 2.1', 'Load 2.1'],
                                      1.2: ['Load 1.2', 'House 1.2', ]}
        for expected_fee, leaf_names in leaf_name_expected_fee.items():
            assert isclose(expected_fee, self.grid_fee_calc.calculate_grid_fee(
                start_market_or_device_name=leaf_names[0],
                target_market_or_device_name=leaf_names[1],
                fee_type=fee_type))
