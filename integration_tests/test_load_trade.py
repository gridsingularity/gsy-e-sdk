"""
Test file for the device client. Depends on d3a test setup file strategy_tests.external_devices
"""
import logging
import json
import traceback
from d3a_api_client.redis_device import RedisDeviceClient
from math import isclose


class TestLoadTrades(RedisDeviceClient):
    def __init__(self, *args, **kwargs):
        self.errors = 0
        self.error_list = []
        self.status = "running"
        self.trade_counter = 0
        super().__init__(*args, **kwargs)

    def on_market_cycle(self, market_info):
        try:
            assert "energy_requirement_kWh" in market_info["device_info"]
            energy_requirement = market_info["device_info"]["energy_requirement_kWh"]
            if energy_requirement > 0.001:
                # Place the bid with a price that will be acceptable for trading
                bid = self.bid_energy(energy_requirement, 33 * energy_requirement)
                bid_info = json.loads(bid["bid"])
                logging.error(bid_info)
                assert bid_info["price"] == 33 * energy_requirement
                assert bid_info["energy"] == energy_requirement

            self.market_info = market_info

        except AssertionError as e:
            logging.error(f"Raised exception: {e}. Traceback: {traceback.format_exc()}")
            self.errors += 1
            self.error_list.append(e)
            raise e

    def on_finish(self, finish_info):
        self.status = "finished"
        self.unregister()

    def on_trade(self, trade_info):
        try:
            assert set(trade_info.keys()) == {"device_info", "event", "event_type", "trade_id", "time", "price",
                                              "energy", "fee_price", "residual_id", "seller", "buyer", "bid_id"}
            assert trade_info["event"] == "trade"
            assert trade_info["event_type"] == "buy"
            assert isclose(trade_info["fee_price"], 0.0)
            assert all(trade_info[member] is not None for member in ["trade_id", "time", "price", "energy"])
            assert isclose(trade_info["price"] / trade_info["energy"], 33.0)
            assert trade_info["residual_id"] == 'None'
            assert trade_info["seller"] == "anonymous"
            assert trade_info["buyer"] == self.area_id
            self.trade_counter += 1
        except AssertionError as e:
            logging.error(f"Raised exception: {e}. Traceback: {traceback.format_exc()}")
            self.errors += 1
            self.error_list.append(e)
            self.status = "finished"
            raise e
