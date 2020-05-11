"""
Test file for the device client. Depends on d3a test setup file strategy_tests.external_devices
"""
import logging
import sys
from time import sleep
from d3a_api_client.rest_device import RestDeviceClient
from d3a_api_client.utils import get_area_uuid_from_area_name_and_collaboration_id


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
            offer = self.offer_energy_rate(market_info["device_info"]["available_energy_kWh"], 16)
            logging.debug(f"Offer placed on the new market: {offer}")
            assert len(self.list_offers()) == 1
        if "energy_requirement_kWh" in market_info["device_info"] and market_info["device_info"]["energy_requirement_kWh"] > 0.0:
            bid = self.bid_energy(market_info["device_info"]["energy_requirement_kWh"] / 2, 30)
            logging.info(f"Bid placed on the new market: {bid}")
            assert len(self.list_bids()) == 1

    def on_tick(self, tick_info):
        logging.debug(f"Progress information on the device: {tick_info}")

    def on_trade(self, trade_info):
        logging.debug(f"Trade info: {trade_info}")

    def on_finish(self, finish_info):
        self.is_finished = True

number_of_areas = 10
load_names = [str(sys.argv[2]) if i == 0 else str(sys.argv[2])+' '+str(i+1) for i in range(number_of_areas)]
pv_names = [str(sys.argv[3]) if i == 0 else str(sys.argv[3])+' '+str(i+1) for i in range(number_of_areas)]
connected_devices = []

for i in range(number_of_areas):
    # Connects one client to the load device
    load = AutoOfferBidOnMarket(
        simulation_id= str(sys.argv[1]), 
        device_id= get_area_uuid_from_area_name_and_collaboration_id(str(sys.argv[1]), load_names[i], 'https://d3aweb-dev.gridsingularity.com'),
        domain_name='https://d3aweb-dev.gridsingularity.com',
        websockets_domain_name='wss://d3aweb-dev.gridsingularity.com/external-ws',
        autoregister=True)
    connected_devices.append(load)
    # Connects a second client to the pv device
    pv = AutoOfferBidOnMarket(
        simulation_id= str(sys.argv[1]), 
        device_id= get_area_uuid_from_area_name_and_collaboration_id(str(sys.argv[1]), pv_names[i], 'https://d3aweb-dev.gridsingularity.com'),
        domain_name='https://d3aweb-dev.gridsingularity.com',
        websockets_domain_name='wss://d3aweb-dev.gridsingularity.com/external-ws',
        autoregister=True)
    connected_devices.append(pv)

# Infinite loop in order to leave the client running on the background
# placing bids and offers on every market cycle.
while not any([connected_device.is_finished for connected_device in connected_devices]):
    sleep(0.5)
