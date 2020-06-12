"""
Test file for the device client. Depends on d3a test setup file strategy_tests.external_devices
"""
import logging
import sys
import argparse
import numpy as np
from time import sleep
from random import randint
from d3a_api_client.rest_device import RestDeviceClient
from d3a_api_client.utils import get_area_uuid_from_area_name_and_collaboration_id

#parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("--simulation_id", type=str, help="Simulation uuid")
parser.add_argument("--load", type=str, help="Load device")
parser.add_argument("--pv", type=str, help="PV device")
parser.add_argument("--researcher_id", type=float, help="Researcher id")
args = parser.parse_args()

number_of_areas = 10
number_of_researcher = 2
connected_devices = []

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
        #sleep(randint(1,10)/100.)
        logging.debug(f"New market information {market_info}")
        if "available_energy_kWh" in market_info["device_info"] and market_info["device_info"]["available_energy_kWh"] > 0.0:
            offer = self.offer_energy_rate(market_info["device_info"]["available_energy_kWh"] / 2, 16)
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

#create a list with the number of areas
areas=range(number_of_areas)
#split the list to have one list for each researcher
#the list contains lists of the devices numbers each researcher will connect to
areas_split = [i.tolist() for i in np.array_split(np.array(areas),number_of_researcher)]

if args.researcher_id <= number_of_researcher:
    #create a list with the names of the Load the researcher connects to 
    load_names = [args.load if i == 0 else f'{args.load} {i+1}' for i in areas_split[int(args.researcher_id-1)]]
    #create a list with the names of the PV the researcher connects to 
    pv_names = [args.pv if i == 0 else f'{args.pv} {i+1}' for i in areas_split[int(args.researcher_id-1)]]
else:
    raise ValueError(f'The researcher id is out of range. The number of researcher is {number_of_researcher}.')

for i in range(len(load_names)):
    # Connects one client to the load device
    load = AutoOfferBidOnMarket(
        simulation_id= args.simulation_id, 
        device_id= get_area_uuid_from_area_name_and_collaboration_id(args.simulation_id, load_names[i], 'https://d3aweb-dev.gridsingularity.com'),
        domain_name='https://d3aweb-dev.gridsingularity.com',
        websockets_domain_name='wss://d3aweb-dev.gridsingularity.com/external-ws',
        autoregister=True)
    connected_devices.append(load)
    # Connects a second client to the pv device
    pv = AutoOfferBidOnMarket(
        simulation_id= args.simulation_id, 
        device_id= get_area_uuid_from_area_name_and_collaboration_id(args.simulation_id, pv_names[i], 'https://d3aweb-dev.gridsingularity.com'),
        domain_name='https://d3aweb-dev.gridsingularity.com',
        websockets_domain_name='wss://d3aweb-dev.gridsingularity.com/external-ws',
        autoregister=True)
    connected_devices.append(pv)

# Infinite loop in order to leave the client running on the background
# placing bids and offers on every market cycle.
while not any([connected_device.is_finished for connected_device in connected_devices]):
    sleep(0.5)
