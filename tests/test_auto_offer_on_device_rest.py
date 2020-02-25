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
            print(market_info["device_info"]["available_energy_kWh"])
            offer = self.offer_energy(market_info["device_info"]["available_energy_kWh"], 0.1)
            logging.debug(f"Offer placed on the new market: {offer}")

    def on_tick(self, tick_info):
        logging.debug(f"Progress information on the device: {tick_info}")

    def on_trade(self, trade_info):
        logging.debug(f"Trade info: {trade_info}")


# Connects one client to the load device
load = AutoOfferBidOnMarket(
    simulation_id='77b0db33-167c-421a-a323-93968a7ee8b8',
    device_id='1613ddab-1413-44c3-a21f-9add93114556',
    domain_name='http://localhost:8000',
    websockets_domain_name='ws://localhost:8000/externalWs',
    is_ssl=False,
    autoregister=True)
# Connects a second client to the pv device
# pv = AutoOfferBidOnMarket('pv', autoregister=True)


# Infinite loop in order to leave the client running on the background
# placing bids and offers on every market cycle.
while True:
    sleep(0.5)
