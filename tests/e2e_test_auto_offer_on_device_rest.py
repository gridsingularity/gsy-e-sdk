"""
Test file for the device client (Load, PV, and Storage). Depends on d3a test setup file strategy_tests.external_devices
"""
import json
import logging

import argparse
from time import sleep
from d3a_api_client.rest_device import RestDeviceClient
from d3a_api_client.utils import get_area_uuid_from_area_name_and_collaboration_id

# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("--simulation_id", type=str, help="Simulation uuid", required=True)
parser.add_argument("--load_names", nargs='+', default=[])
parser.add_argument("--pv_names", nargs='+', default=[])
parser.add_argument("--storage_names", nargs='+', default=[])
args = parser.parse_args()
connected_devices = []

domain_name = 'https://d3aweb-staging.gridsingularity.com'
websockets_domain_name = 'wss://d3aweb-staging.gridsingularity.com/external-ws'


class AutoOfferBidOnMarket(RestDeviceClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_cycle(self, market_info):
        """
        Places a bid or an offer whenever a new market is created. The amount of energy
        for the bid/offer depends on the available energy of the PV, or on the required
        energy of the load.
        :param market_info: Incoming message containing the newly-created market info
        :return: None
        """
        if self.registered is False or self.is_finished is True:
            return
        logging.debug(f"New market information {market_info}")
        if "available_energy_kWh" in market_info["device_info"] and market_info["device_info"]["available_energy_kWh"] > 0.0:
            offered_energy = market_info["device_info"]["available_energy_kWh"]
            offered_rate = 28*offered_energy
            offer_response = self.offer_energy_rate(
                offered_energy, offered_rate)
            expected_price = offered_rate*offered_energy
            logging.info("*****************Offer placed in the market by area_uuid: " + market_info["area_uuid"])
            logging.info(f"Offer placed in the market: {offer_response}")
            logging.info("******************************************************")
            assert offer_response["status"] == "ready"
            offer_dict = json.loads(offer_response["offer"])
            assert offer_dict["energy"] == offered_energy
            assert offer_dict["price"] == expected_price

        if "energy_requirement_kWh" in market_info["device_info"] and market_info["device_info"]["energy_requirement_kWh"] > 0.0:
            bid_energy = market_info["device_info"]["energy_requirement_kWh"]
            bid_price = 28
            bid_response = self.bid_energy(bid_energy, bid_price)
            logging.info("*********Bid placed in the market by area_uuid: " + market_info["area_uuid"])
            logging.info(f"Bid placed in the market: {bid_response}")
            logging.info("***************************************************")
            assert bid_response["status"] == "ready"
            bid_dict = json.loads(bid_response["bid"])
            assert bid_dict["energy"] == bid_energy
            assert bid_dict["price"] == bid_price
        # for storage to offer
        if "energy_to_sell" in market_info["device_info"] and market_info["device_info"]["energy_to_sell"] > 0.0:
            offered_energy_storage = market_info["device_info"]["energy_to_sell"]
            offered_rate_storage = 16
            offer_response_storage = self.offer_energy_rate(
                offered_energy_storage, offered_rate_storage)
            expected_price_storage = offered_rate_storage*offered_energy_storage
            logging.info("*****************Offer placed in the market by area_uuid: " + market_info["area_uuid"])
            logging.info(f"Offer placed in the market: {offer_response_storage}")
            logging.info("******************************************************")
            assert offer_response_storage["status"] == "ready"
            offer_storage_dict = json.loads(offer_response_storage["offer"])
            assert offer_storage_dict["energy"] == offered_energy_storage
            assert offer_storage_dict["price"] == expected_price_storage
        # for storage to buy
        if "energy_to_buy" in market_info["device_info"] and market_info["device_info"]["energy_to_buy"] > 0.0:
            bid_energy_storage = market_info["device_info"]["energy_to_buy"]
            bid_price_storage = 30
            bid_response_storage = self.bid_energy(bid_energy_storage, bid_price_storage)
            logging.info("*********Bid placed in the market by area_uuid: " + market_info["area_uuid"])
            logging.info(f"Bid placed in the market: {bid_response_storage}")
            logging.info("***************************************************")
            assert bid_response_storage["status"] == "ready"
            bid_Storage_dict = json.loads(bid_response_storage["bid"])
            assert bid_Storage_dict["energy"] == bid_energy_storage
            assert bid_Storage_dict["price"] == bid_price_storage

    def on_tick(self, tick_info):
        logging.debug(f"Progress information on the device: {tick_info}")

    def on_trade(self, trade_info):
        logging.debug(f"Trade info: {trade_info}")

    def on_finish(self, finish_info):
        self.is_finished = True


for load_name in args.load_names:
    # Connects client to the load device
    logging.info("Inside Load Loop")
    load = AutoOfferBidOnMarket(
        simulation_id=args.simulation_id,
        device_id=get_area_uuid_from_area_name_and_collaboration_id(
            args.simulation_id, load_name, domain_name),
        domain_name=domain_name,
        websockets_domain_name=websockets_domain_name,
        autoregister=True)
    connected_devices.append(load)
for pv_name in args.pv_names:
    # Connects a client to the pv device
    logging.info("Inside PV Loop")
    pv = AutoOfferBidOnMarket(
        simulation_id=args.simulation_id,
        device_id=get_area_uuid_from_area_name_and_collaboration_id(
            args.simulation_id, pv_name, domain_name),
        domain_name=domain_name,
        websockets_domain_name=websockets_domain_name,
        autoregister=True)
    connected_devices.append(pv)
for storage_name in args.storage_names:
    # Connects a client to the storage device
    logging.info("Inside Storage Loop")
    pv = AutoOfferBidOnMarket(
        simulation_id=args.simulation_id,
        device_id=get_area_uuid_from_area_name_and_collaboration_id(
            args.simulation_id, storage_name, domain_name),
        domain_name=domain_name,
        websockets_domain_name=websockets_domain_name,
        autoregister=True)
    connected_devices.append(pv)
# Infinite loop in order to leave the client running on the background
# placing bids and offers on every market cycle.
logging.info('While loop starting')
while not any([connected_device.is_finished for connected_device in connected_devices]):
    sleep(0.5)
