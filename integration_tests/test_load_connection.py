"""
Test file for the device client. Depends on d3a test setup file strategy_tests.external_devices
"""
import logging
import json
import traceback
from d3a_api_client.redis_device import RedisDeviceClient
from time import sleep


class AutoBidOnLoadDevice(RedisDeviceClient):
    def __init__(self, *args, **kwargs):
        self.errors = 0
        self.error_list = []
        self.status = None
        self.latest_stats = {}
        self.market_info = {}
        self.device_bills = {}
        self.final_device_bill = {}
        super().__init__(*args, **kwargs)

    def on_market_cycle(self, market_info):
        try:
            sleep(1)
            self.market_info = market_info
            logging.info(f"market_info: {self.market_info}")
            assert "energy_requirement_kWh" in market_info["device_info"], \
                "energy_requirement_kWh is not in market info"
            energy_requirement = market_info["device_info"]["energy_requirement_kWh"]
            if energy_requirement > 0.001:
                self.delete_bid()
                sleep(1)
                bid = self.bid_energy(energy_requirement, 33 * energy_requirement)
                sleep(1)
                bid_info = json.loads(bid["bid"])
                assert bid_info["price"] == 33 * energy_requirement, "BID PRICE is not as expected"
                assert bid_info["energy"] == energy_requirement, "BID ENERGY is not as expected"
            assert "device_bill" in market_info, "device_bill key not found"
            self.device_bills = self.market_info["device_bill"]
            logging.info(f"device_bills: {self.device_bills}")
            assert set(self.device_bills.keys()) == \
                   {'bought', 'sold', 'spent', 'earned', 'total_energy', 'total_cost', 'market_fee',
                    'type', 'penalty_energy', 'penalty_cost'}
            assert "last_market_stats" in market_info
            assert set(market_info["last_market_stats"]) == \
                   {'min_trade_rate', 'max_trade_rate', 'avg_trade_rate', 'median_trade_rate',
                    'total_traded_energy_kWh'}

            assert "start_time" in market_info, "start_time key not found"
            if market_info["start_time"][-5:] == "23:00":
                self.status = "finished"
                self.market_info = market_info
                self.final_device_bill = self.market_info["device_bill"]
                logging.info(f"FINALBOUGHT: {self.final_device_bill['bought']}")
            logging.info(f"status: {self.status}")
            logging.info(f"ERRORS: {self.errors}")
            logging.info(f"BOUGHT: {self.device_bills['bought']}")

        except AssertionError as e:
            logging.error(f"Raised exception: {e}. Traceback: {traceback.format_exc()}")
            self.errors += 1
            self.error_list.append(e)
            raise e
