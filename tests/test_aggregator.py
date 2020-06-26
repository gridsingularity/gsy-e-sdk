# TODO: An integration test needs to be added for this test script.
import logging
import sys
from time import sleep
from d3a_api_client.aggregator import Aggregator


class AutoAggregator(Aggregator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_cycle(self, market_info):
        # TODO: to be implemented in d3-web: add "device_data" into event message
        if self.registered is False or self.is_finished is True:
            return
        batch_command_dict = {}
        for device_uuid, market_info_device in market_info["device_data"].items():
            logging.debug(f"Progress information on the device: {market_info_device}")
            batch_command_dict[device_uuid] = []
            if "available_energy_kWh" in market_info_device["device_info"] and market_info_device["device_info"]["available_energy_kWh"] > 0.0:
                offer_energy = market_info["device_info"]["available_energy_kWh"]
                offer_price = offer_energy * 16
                batch_command_dict[device_uuid].append({"type": "offer", "energy": offer_energy, "price": offer_price})
            if "energy_requirement_kWh" in market_info_device["device_info"] and market_info_device["device_info"]["energy_requirement_kWh"] > 0.0:
                bid_energy = market_info["device_info"]["available_energy_kWh"] / 2
                bid_price = bid_energy * 30
                batch_command_dict[device_uuid].append({"type": "bid", "energy": bid_energy, "price": bid_price})
        # Send batch commands
        batch_response = self.batch_command(batch_command_dict)
        # Print response logs of all commands in batch
        test_posted_batch_commands = {}
        for device_uuid, batch_response_device in batch_response["device_data"].items():
            if batch_response_device["type"] == "offer":
                logging.debug(f"Offer placed on the new market: {batch_response_device['offer']}")
                test_posted_batch_commands[device_uuid] = [{"type": "list-offers"}]
            if batch_response_device["type"] == "bid":
                logging.debug(f"Offer placed on the new market: {batch_response_device['bid']}")
                test_posted_batch_commands[device_uuid] = [{"type": "list-bids"}]
        # List offers and bids to validate that they were indeed offered
        test_batch_response = self.batch_command(test_posted_batch_commands)
        for device_uuid, test_batch_response_device in test_batch_response.items():
            if test_batch_response_device["type"] == "list-offers":
                assert len(test_batch_response_device["offer_list"]) == 1
            if test_batch_response_device["type"] == "list-bids":
                assert len(test_batch_response_device["bid_list"]) == 1

    def on_tick(self, tick_info):
        # TODO: to be implemented in d3-web: add "device_data" into event message
        for device_uuid in tick_info["device_data"].keys():
            logging.debug(f"Progress information on the device: {tick_info['device_data'][device_uuid]}")

    def on_trade(self, trade_info):
        # TODO: to be implemented in d3a-web: redirect trade event also to connected aggregator
        # The device_uuid will be part of the trade_info
        logging.debug(f"Trade info: {trade_info}")

    def on_finish(self, finish_info):
        # TODO: to be implemented in d3-web: add "device_data" into event message
        # Set to finished if all connected devices are reported finished
        if set(finish_info["device_data"].keys()) == set(self.device_uuid_list):
            self.is_finished = True

import os
os.environ["API_CLIENT_USERNAME"] = "muhammad@gridsingularity.com"
os.environ["API_CLIENT_PASSWORD"] = "muhammadtest321*"


aggregator = AutoAggregator(
    simulation_id=str(),
    domain_name='http://localhost:8000/',
    aggregator_name="faizan_aggregator",
    websockets_domain_name='ws://localhost:8000/external-ws',
    autoregister=True)

while not aggregator.is_finished:
    sleep(0.5)
