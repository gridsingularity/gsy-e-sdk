"""
Test file for the device client. Depends on d3a test setup file strategy_tests.external_devices
"""
import logging
import os
from time import sleep
from d3a_api_client.rest_device import RestDeviceClient
from d3a_api_client.utils import get_area_uuid_from_area_name_and_collaboration_id

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)


class AutoSendForecast(RestDeviceClient):
    forecast = 0

    def on_market_cycle(self, market_info):
        """
        Sends increasing energy forecast to pv and load devices
        """
        root_logger.debug(f"New market information {market_info}")
        self.forecast += 50
        if "available_energy_kWh" in market_info["device_info"]:
            root_logger.info(f"self.set_pv_power_forecast({self.forecast})")
            response = self.set_power_forecast(self.forecast)

        if "energy_requirement_kWh" in market_info["device_info"]:
            root_logger.info(f"self.set_load_power_forecast({self.forecast})")
            response = self.set_power_forecast(self.forecast)

    def on_tick(self, tick_info):
        logging.debug(f"Progress information on the device: {tick_info}")

    def on_trade(self, trade_info):
        logging.debug(f"Trade info: {trade_info}")


os.environ["API_CLIENT_USERNAME"] = ""
os.environ["API_CLIENT_PASSWORD"] = ""
simulation_id = "4de1f0b4-f2bc-4c78-8cb9-a9941cab3a46"
domain_name = "http://localhost:8000"
websocket_domain_name = 'ws://localhost:8000/external-ws'

device_args = {
    "simulation_id": simulation_id,
    "domain_name": domain_name,
    "websockets_domain_name": websocket_domain_name,
    "autoregister": True
}

load_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    device_args["simulation_id"], "load", device_args["domain_name"])
device_args["device_id"] = load_uuid
load = AutoSendForecast(
    **device_args)

pv_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    device_args["simulation_id"], "pv", device_args["domain_name"])
device_args["device_id"] = pv_uuid
pv = AutoSendForecast(
    **device_args)

while True:
    sleep(0.5)
