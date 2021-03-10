"""
Test file for the device client. Depends on d3a test setup file strategy_tests.external_ess_bids
"""
import json
import traceback
import logging
from d3a_api_client.types import device_client_type


class AutoBidOnESSDevice(device_client_type):
    def __init__(self, *args, **kwargs):
        self.errors = 0
        self.error_list = []
        self.last_market_info = None
        self.status = "running"
        self.latest_stats = {}
        super().__init__(*args, **kwargs)

    def on_market_cycle(self, market_info):
        try:
            assert set(market_info["device_info"].keys()) == \
                   {'energy_active_in_offers', 'total_cost', 'energy_active_in_bids',
                    'energy_to_sell', 'used_storage', 'energy_traded', 'free_storage', 'energy_to_buy'}
            energy = market_info["device_info"]["energy_to_buy"]
            if energy > 0:
                bid = self.bid_energy(energy, (31 * energy))
                bid_info = json.loads(bid["bid"])
                assert bid_info['buyer_origin'] == self.device_id
                assert bid_info['buyer_origin_id'] == bid_info['buyer_id'] is not None
                assert bid_info["price"] == 31 * energy
                assert bid_info["energy"] == energy
                device_info = self.device_info()
                assert device_info["device_info"]["energy_to_buy"] == 0.0
                assert device_info["device_info"]["energy_active_in_offers"] == energy

            if market_info["start_time"][-5:] == "23:00":
                self.last_market_info = market_info
                self.status = "finished"
                self.unregister()
        except AssertionError as e:
            logging.error(f"Raised exception: {e}. Traceback: {traceback.format_exc()}")
            self.error_list.append(e)
            self.errors += 1
            raise e
