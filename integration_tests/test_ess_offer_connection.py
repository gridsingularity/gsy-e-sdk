"""
Test file for the device client. Depends on d3a test setup file strategy_tests.external_devices
"""
import json
import traceback
import logging
from d3a_api_client.redis_device import RedisDeviceClient
from time import sleep


class AutoOfferOnESSDevice(RedisDeviceClient):
    def __init__(self, *args, **kwargs):
        self.errors = 0
        self.status = "running"
        self.latest_stats = {}
        super().__init__(*args, **kwargs)

    def on_market_cycle(self, market_info):
        try:
            assert "used_storage" in market_info
            print(f"market_info: {market_info}")
            if market_info["used_storage"] > \
                    market_info["min_allowed_soc_ratio"] * market_info["capacity"]:
                energy = market_info["max_abs_battery_power_kW"] * 0.25
                offer = self.offer_energy(energy, (10 * energy))
                offer_info = json.loads(offer["offer"])
                assert offer_info["price"] == 10 * energy
                assert offer_info["energy"] == energy

            if market_info["start_time"][-5:] == "23:45":
                self.status = "finished"
        except AssertionError as e:
            logging.error(f"Raised exception: {e}. Traceback: {traceback.format_exc()}")
            self.errors += 1
            raise e


r = AutoOfferOnESSDevice('storage', autoregister=True)

while True:
    sleep(1)
