"""
Test file for the market client. Depends on d3a test setup file strategy_tests.external_devices
"""
import logging
import traceback
from pendulum import today

from d3a_interface.constants_limits import DATE_TIME_FORMAT
from d3a_api_client.types import market_client_type


class AutoGridFeeUpdateOnMarket(market_client_type):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.errors = 0
        self.error_list = []
        self.dso_info = {}
        self.status = "running"
        self.fee_profile = {today().add(minutes=i * 60).format(DATE_TIME_FORMAT): round(i / 10, 3)
                            for i in range(25)}
        self.current_time = None
        self.updated_fee = None
        self.list_dso_stats = None

    def on_market_cycle(self, market_info):

        try:
            logging.debug(f"New market information {market_info}")
            assert set(market_info.keys()) == {'status', 'event', 'market_info'}
            self.current_time = market_info['market_info']['start_time']
            expected_grid_fee = self.fee_profile[self.current_time]
            self.updated_fee = self.grid_fees(expected_grid_fee)
            assert set(self.updated_fee.keys()) == \
                   {'transaction_id', 'area_uuid', 'market_fee_const', 'status', 'command'}
            logging.debug(f"updated_fee: {self.updated_fee}")
            assert float(self.updated_fee["market_fee_const"]) == expected_grid_fee
            self.list_dso_stats = self.last_market_dso_stats()
            assert set(self.list_dso_stats.keys()) == \
                   {'name', 'command', 'area_uuid', 'status', 'transaction_id', 'market_stats'}


        except AssertionError as e:
            logging.error(f"Raised exception: {e}. Traceback: {traceback.format_exc()}")
            self.errors += 1
            self.error_list.append(e)
            raise e

    def on_finish(self, finish_info):
        self.status = "finished"

# Connects one client to the house-2 market
# market = AutoGridFeeUpdateOnMarket('house-2')


# Infinite loop in order to leave the client running on the background
# placing bids and offers on every market cycle.
# while True:
#     from time import sleep
#     sleep(0.5)
#     if market.status == "finished":
#         break
