"""

API CLIENT INSTALLATION STEPS
mkvirtualenv test-client
pip install git+https://github.com/gridsingularity/d3a-api-client.git
python test_connection_to_dev.py

"""
import logging
from time import sleep
from d3a_api_client.rest_device import RestDeviceClient
import json


class AutoOfferBidOnMarket(RestDeviceClient):

    def on_market_cycle(self, market_info):
        """
        Places a bid or an offer whenever a new market is created. The amount of energy
        for the bid/offer depends on the available energy of the PV, or on the required
        energy of the load.
        :param market_info: Incoming message containing the newly-created market info
        :return: None
        """
        if self.registered is False:
            print("not registered yet")
            return
        else:
            print("registered!!!")
            lb = self.list_bids()
            # print("after list bids")
            # print(lb)
            # print("---")

        self.market_info = market_info
        # logging.debug(f"New market information {market_info}")
        # if "available_energy_kWh" in market_info["device_info"] and market_info["device_info"]["available_energy_kWh"] > 0.0:
        #     offer = self.offer_energy(market_info["device_info"]["available_energy_kWh"] / 2, 0.1)
        #     logging.debug(f"Offer placed on the new market: {offer}")
        #     assert len(self.list_offers()) == 1
        # if "energy_requirement_kWh" in market_info["device_info"] and market_info["device_info"]["energy_requirement_kWh"] > 0.0:
        #     bid = self.bid_energy(market_info["device_info"]["energy_requirement_kWh"] / 2, 30)
        #     logging.error(f"Bid placed on the new market: {bid}")
        #     bid = self.bid_energy(market_info["device_info"]["energy_requirement_kWh"] / 2, 30)
        #     logging.error(f"Bid placed on the new market: {bid}")
        #     print(self.delete_bid())
        #     print(self.list_bids())
        #     assert len(self.list_bids()) == 0
        #     print("---------")

    def on_tick(self, tick_info):
        if "energy_requirement_kWh" in self.market_info["device_info"] and self.market_info["device_info"]["energy_requirement_kWh"] > 0.0:
            print("----tick begin-----")
            half_energy = self.market_info["device_info"]["energy_requirement_kWh"] / 2
            price = 30 * half_energy
            print("half_energy", half_energy, "price", price)
            bid = self.bid_energy(half_energy, price)
            self.market_info["device_info"]["energy_requirement_kWh"] -= half_energy
            logging.error(f"Bid placed on the new market: {bid}")
            bid = self.bid_energy(half_energy, price)
            self.market_info["device_info"]["energy_requirement_kWh"] -= half_energy
            # print(bid)
            if not "error" in bid.keys():
                logging.error(f"Bid placed on the new market: {bid}")
                # logging.error(f"Deleting all bids ...")
                # print(bid)
                # logging.error(f'deleting {bid["bid"]["id"]}'
                bid_id = json.loads(bid["bid"])["id"]
                db = self.delete_bid(bid_id=bid_id)
                print("after bid delete")
                lb = self.list_bids()
                print("after list bids")
                print("db", db)
                print("lb", lb)
                # assert len(lb) == 0
                print("####eeeend#####")
            print("----tick ended-----")

        # logging.debug(f"Progress information on the device: {tick_info}")

    def on_trade(self, trade_info):
        logging.debug(f"Trade info: {trade_info}")


from d3a_api_client.utils import get_area_uuid_from_area_name_and_collaboration_id
import os

# os.environ['API_CLIENT_USERNAME'] = "hannes@gridsingularity.com"
os.environ['API_CLIENT_USERNAME'] = "diedrichhannes@gmail.com"
os.environ['API_CLIENT_PASSWORD'] = "d3a_sim_hd"
collab_id = "797dafd2-bf1c-4ca3-9385-9d7a7eaa6b27"
device_name = "Load"
# domain_name = 'https://d3aweb-dev.gridsingularity.com'
# wss_domain = 'wss://d3aweb-dev.gridsingularity.com/external-ws'
domain_name = 'http://localhost:8000'
wss_domain = 'ws://localhost:8000/external-ws'
area_uuid = get_area_uuid_from_area_name_and_collaboration_id(collab_id, device_name, domain_name)
# Connects one client to the load device
load = AutoOfferBidOnMarket(
    simulation_id=collab_id,
    device_id=area_uuid,
    domain_name=domain_name,
    websockets_domain_name=wss_domain,
    autoregister=True)

# Infinite loop in order to leave the client running on the background
# placing bids and offers on every market cycle.
while True:
    sleep(0.5)
