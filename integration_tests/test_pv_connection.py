"""
Test file for the device client. Depends on d3a test setup file strategy_tests.external_devices
"""
import json
import traceback
import logging
from d3a_api_client.redis_device import RedisDeviceClient


class AutoOfferOnPVDevice(RedisDeviceClient):
    def __init__(self, *args, **kwargs):
        self.errors = 0
        self.error_list = []
        self.status = "running"
        self.latest_stats = {}
        self.market_info = {}
        self.device_bills = {}
        super().__init__(*args, **kwargs)

    def on_market_cycle(self, market_info):
        try:
            assert "available_energy_kWh" in market_info["device_info"]
            available_energy = market_info["device_info"]["available_energy_kWh"]
            if available_energy > 0.0:
                # Placing an expensive offer to the market that will not be accepted
                offer = self.offer_energy(available_energy, 50 * available_energy)
                offer_info = json.loads(offer["offer"])
                assert offer_info["price"] == 50 * available_energy
                assert offer_info["energy"] == available_energy
                # Validate that the offer was placed to the market
                offer_listing = self.list_offers()
                listed_offer = next(offer for offer in offer_listing["offer_list"] if offer["id"] == offer_info["id"])
                assert listed_offer["price"] == listed_offer["price"]
                assert listed_offer["energy"] == listed_offer["energy"]
                # Try to delete the offer
                delete_resp = self.delete_offer(offer_info["id"])
                assert delete_resp["deleted_offers"] == [offer_info["id"]]
                # Validate that the offer was deleted from the market
                empty_listing = self.list_offers()
                assert not any(o for o in empty_listing["offer_list"] if o["id"] == offer_info["id"])
                # Place the offer with a price that will be acceptable for trading
                offer = self.offer_energy(available_energy, 10 * available_energy)
                offer_info = json.loads(offer["offer"])
                assert offer_info["price"] == 10 * available_energy
                assert offer_info["energy"] == available_energy

            assert "device_bill" in market_info
            self.device_bills = market_info["device_bill"]
            logging.info(f"device_bills: {self.device_bills.keys()}")
            assert set(self.device_bills.keys()) == \
                   {'bought', 'sold', 'spent', 'earned', 'total_energy', 'total_cost',
                    'market_fee', 'type', 'penalty_energy', 'penalty_cost'}
            assert "last_market_stats" in market_info
            logging.info(f"last_market_stats: {market_info['last_market_stats']}")
            assert set(market_info["last_market_stats"].keys()) == \
                   {'min_trade_rate', 'max_trade_rate', 'avg_trade_rate', 'median_trade_rate',
                    'total_traded_energy_kWh'}

            if market_info["start_time"][-5:] == "23:00":
                self.status = "finished"
                self.unregister()

            self.market_info = market_info

        except AssertionError as e:
            logging.error(f"Raised exception: {e}. Traceback: {traceback.format_exc()}")
            self.errors += 1
            self.error_list.append(e)
            raise e
