"""
Test file for the device client. Depends on d3a test setup file strategy_tests.external_devices
"""
import logging
from time import sleep
from d3a_api_client.rest_device import RestDeviceClient


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
            return
        logging.debug(f"New market information {market_info}")
        if "available_energy_kWh" in market_info["device_info"] and market_info["device_info"]["available_energy_kWh"] > 0.0:
            offer = self.offer_energy(market_info["device_info"]["available_energy_kWh"] / 2, 0.1)
            logging.debug(f"Offer placed on the new market: {offer}")
            assert len(self.list_offers()) == 1
        if "energy_requirement_kWh" in market_info["device_info"] and market_info["device_info"]["energy_requirement_kWh"] > 0.0:
            bid = self.bid_energy(market_info["device_info"]["energy_requirement_kWh"], 30)
            logging.error(f"Bid placed on the new market: {bid}")
            assert len(self.list_bids()) == 1

    def on_tick(self, tick_info):
        logging.debug(f"Progress information on the device: {tick_info}")

    def on_trade(self, trade_info):
        logging.debug(f"Trade info: {trade_info}")


# Connects one client to the load device
load = AutoOfferBidOnMarket(
    simulation_id="00555be7-bcc6-4b9b-bb9b-5150300b5f9d",
    device_id='fde7c16f-f885-4ed8-ab3e-a1bbebeb188f',
    domain_name='https://d3aweb-dev.gridsingularity.com',
    websockets_domain_name='wss://d3aweb-dev.gridsingularity.com/external-ws',
    autoregister=True)
# Connects a second client to the pv device
# pv = AutoOfferBidOnMarket('pv', autoregister=True)


# Infinite loop in order to leave the client running on the background
# placing bids and offers on every market cycle.
while True:
    sleep(0.5)
