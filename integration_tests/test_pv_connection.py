"""
Test file for the device client. Depends on d3a test setup file strategy_tests.external_devices
"""
import json
import traceback
import logging
from math import isclose
from d3a_api_client.redis_device import RedisDeviceClient


class AutoOfferOnPVDevice(RedisDeviceClient):
    def __init__(self, *args, **kwargs):
        self.errors = 0
        self.status = "running"
        self.latest_stats = {}
        super().__init__(*args, **kwargs)

    def on_market_cycle(self, market_info):
        try:
            assert "available_energy_kWh" in market_info
            if market_info["available_energy_kWh"] > 0.0:
                # Placing an expensive offer to the market that will not be accepted
                offer = self.offer_energy(market_info["available_energy_kWh"], 50 * market_info["available_energy_kWh"])
                offer_info = json.loads(offer["offer"])
                assert offer_info["price"] == 50 * market_info["available_energy_kWh"]
                assert offer_info["energy"] == market_info["available_energy_kWh"]
                # Validate that the bid was placed to the market
                offer_listing = self.list_offers()
                listed_offer = next(bid for bid in offer_listing["offer_list"] if bid["id"] == offer_info["id"])
                assert listed_offer["price"] == listed_offer["price"]
                assert listed_offer["energy"] == listed_offer["energy"]
                # Try to delete the bid
                delete_resp = self.delete_offer(offer_info["id"])
                assert delete_resp["deleted_offer"] == offer_info["id"]
                # Validate that the bid was deleted from the market
                empty_listing = self.list_bids()
                assert not any(o for o in empty_listing["offer_list"] if o["id"] == offer_info["id"])
                # Place the bid with a price that will be acceptable for trading
                offer = self.offer_energy(market_info["available_energy_kWh"], 10 * market_info["available_energy_kWh"])
                offer_info = json.loads(offer["offer"])
                assert offer_info["price"] == 10 * market_info["available_energy_kWh"]
                assert offer_info["energy"] == market_info["available_energy_kWh"]

            stats = self.list_device_stats()
            traded_slots = stats["market_stats"]["energy_trade_profile"]["sold_energy"]["pv"]["accumulated"].values()
            assert isclose(stats["device_stats"]["bills"]["bought"], sum(traded_slots))

            if market_info["start_time"][-5:] == "23:45":
                self.status = "finished"
            self.latest_stats = stats
        except AssertionError as e:
            logging.error(f"Raised exception: {e}. Traceback: {traceback.format_exc()}")
            self.errors += 1
            raise e
