"""
Test file for the device client. Depends on d3a test setup file strategy_tests.external_ess_bids
"""
import json
import traceback
import logging
from d3a_api_client.redis_device import RedisDeviceClient
from time import sleep


class AutoBidOnESSDevice(RedisDeviceClient):
    def __init__(self, *args, **kwargs):
        self.errors = 0
        self.error_list = []
        self.last_market_info = None
        self.status = "running"
        self.latest_stats = {}
        super().__init__(*args, **kwargs)

    def on_market_cycle(self, market_info):
        try:
            assert "energy_to_buy_dict" in market_info
            energy = market_info["energy_to_buy_dict"]
            if energy > 0:
                bid = self.bid_energy(energy, (31 * energy))
                bid_info = json.loads(bid["bid"])
                assert bid_info["price"] == 31 * energy
                assert bid_info["energy"] == energy

            if market_info["start_time"][-5:] == "23:00":
                self.last_market_info = market_info
                self.status = "finished"
        except AssertionError as e:
            logging.error(f"Raised exception: {e}. Traceback: {traceback.format_exc()}")
            self.error_list.append(e)
            self.errors += 1
            raise e

