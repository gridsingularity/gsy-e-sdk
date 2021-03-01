"""
Test file for the device client. Depends on d3a test setup file strategy_tests.external_ess_offers
"""
import json
import traceback
import logging
from d3a_api_client.types import device_client_type


class AutoOfferOnESSDevice(device_client_type):
    def __init__(self, *args, **kwargs):
        self.errors = 0
        self.error_list = []
        self.status = "running"
        self.last_market_info = None
        self.latest_stats = {}
        super().__init__(*args, **kwargs)

    def on_market_cycle(self, market_info):
        try:
            assert set(market_info["asset_info"].keys()) == \
                   {'energy_to_sell','offered_sell_kWh', 'energy_to_buy', 'offered_buy_kWh',
                    'free_storage', 'used_storage'}
            energy_to_sell = market_info["asset_info"]["energy_to_sell"]
            if energy_to_sell > 0:
                offer = self.offer_energy(energy_to_sell, (10 * energy_to_sell))
                offer_info = json.loads(offer["offer"])
                assert offer_info["price"] == 10 * energy_to_sell
                assert offer_info["energy"] == energy_to_sell
                asset_info = self.asset_info()
                assert asset_info["energy_to_sell"] == 0.0
                assert asset_info["offered_sell_kWh"] == energy_to_sell


            if market_info["start_time"][-5:] == "23:00":
                self.status = "finished"
                self.last_market_info = market_info
                self.unregister()
        except AssertionError as e:
            logging.error(f"Raised exception: {e}. Traceback: {traceback.format_exc()}")
            self.errors += 1
            self.error_list.append(e)
            raise e

