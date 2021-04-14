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
        self.status = "running"
        self.last_market_info = None
        self.latest_stats = {}
        super().__init__(*args, **kwargs)

    def on_market_cycle(self, market_info):
        try:
            assert set(market_info["device_info"].keys()) == \
                   {'energy_active_in_offers', 'total_cost', 'energy_active_in_bids',
                    'energy_to_sell', 'used_storage', 'energy_traded', 'free_storage', 'energy_to_buy'}
            energy_to_sell = market_info["device_info"]["energy_to_sell"]
            if energy_to_sell > 0:
                offer_price = 10 * energy_to_sell
                offer = self.offer_energy(energy_to_sell, offer_price)
                offer_info = json.loads(offer["offer"])
                assert offer_info['seller_origin'] == self.device_id
                assert offer_info['seller_origin_id'] == offer_info['seller_id'] is not None
                assert offer_info["price"] == offer_price
                assert offer_info["energy"] == energy_to_sell
                device_info = self.device_info()
                assert device_info["device_info"]["energy_to_sell"] == 0.0
                assert device_info["device_info"]["offered_sell_kWh"] == energy_to_sell

            self.last_market_info = market_info

        except Exception as e:
            logging.error(f"Raised exception: {e}. Traceback: {traceback.format_exc()}")
            self.errors += 1
            raise e

    def on_finish(self, finish_info):
        self.status = "finished"
        self.unregister()

