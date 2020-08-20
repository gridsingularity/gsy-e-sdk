import os
import logging
from time import sleep

from d3a_api_client.rest_device import RestDeviceClient
from d3a_api_client.utils import get_area_uuid_from_area_name_and_collaboration_id

simulation_id = 'c8b30949-88ed-4ec8-a2c1-5b013ad9e2e3' # TODO update simulation id with experiment id (if RUN_ON_D3A_WEB is TRUE)
domain_name = 'http://localhost:8000' # leave as is
websocket_domain_name = 'ws://localhost:8000/external-ws' # leave as is

os.environ["API_CLIENT_USERNAME"] = "other@user.com"
os.environ["API_CLIENT_PASSWORD"] = "other_user"


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
        print("new market")
        # assert False
        if self.registered is False or self.is_finished is True:
            print("not yet registered")
            # return 2
        # assert False, "test"
        # raise Exception("test")
        logging.info(f"New market information {market_info}")
        # if "available_energy_kWh" in market_info["device_info"] and market_info["device_info"]["available_energy_kWh"] > 0.0:
        #     offer = self.offer_energy_rate(market_info["device_info"]["available_energy_kWh"] / 2, 16)
        #     logging.debug(f"Offer placed on the new market: {offer}")
        #     assert len(self.list_offers()) == 1
        if "energy_requirement_kWh" in market_info["device_info"] and market_info["device_info"]["energy_requirement_kWh"] > 0.0:
            bid = self.bid_energy(market_info["device_info"]["energy_requirement_kWh"] / 2, 30)
            logging.info(f"Bid placed on the new market: {bid}")
            assert len(self.list_bids()) == 1
        # return 2

    def on_tick(self, tick_info):
        logging.debug(f"Progress information on the device: {tick_info}")

    def on_trade(self, trade_info):
        logging.debug(f"Trade info: {trade_info}")

    def on_finish(self, finish_info):
        self.is_finished = True

load_name = "Load"
load_uuid = get_area_uuid_from_area_name_and_collaboration_id(
                            simulation_id, load_name,
                            domain_name)
print("load_uuid", load_uuid)

device = AutoOfferBidOnMarket(device_id=load_uuid,
                          autoregister=True,
                          simulation_id=simulation_id,
                          domain_name=domain_name,
                          websockets_domain_name=websocket_domain_name)

while not device.is_finished:
    sleep(0.5)