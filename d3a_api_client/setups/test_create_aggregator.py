from pendulum import today
import logging
from time import sleep
from d3a_api_client.aggregator import Aggregator
from d3a_api_client.rest_device import RestDeviceClient
from d3a_api_client.utils import get_area_uuid_from_area_name_and_collaboration_id
from d3a_interface.constants_limits import DATE_TIME_FORMAT
from d3a_api_client.rest_market import RestMarketClient


class TestAggregator(Aggregator):

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
        if self.is_finished is True:
            return
        if "content" not in market_info:
            return

        for device_event in market_info["content"]:
            if "available_energy_kWh" in device_event["device_info"] and \
                    device_event["device_info"]["available_energy_kWh"] > 0.0:
                self.add_to_batch_commands.offer_energy(device_event["area_uuid"], price=1,
                                                        energy=device_event["device_info"]["available_energy_kWh"] / 2)
                self.add_to_batch_commands.list_offers(device_event["area_uuid"])

            if "energy_requirement_kWh" in device_event["device_info"] and \
                    device_event["device_info"]["energy_requirement_kWh"] > 0.0:
                self.add_to_batch_commands.bid_energy(device_event["area_uuid"], price=30,
                                                      energy=device_event["device_info"]["energy_requirement_kWh"] / 2)
                self.add_to_batch_commands.list_bids(device_event["area_uuid"])

            response = self.execute_batch_commands()
            logging.debug(f"Batch command placed on the new market: {response}")

    def on_tick(self, tick_info):
        logging.debug(f"Progress information on the device: {tick_info}")

    def on_trade(self, trade_info):
        logging.debug(f"Trade info: {trade_info}")

    def on_finish(self, finish_info):
        self.is_finished = True


import os
simulation_id = os.environ["API_CLIENT_SIMULATION_ID"]
domain_name = os.environ["API_CLIENT_DOMAIN_NAME"]
websocket_domain_name = os.environ["API_CLIENT_WEBSOCKET_DOMAIN_NAME"]


aggr = TestAggregator(
    simulation_id=simulation_id,
    domain_name=domain_name,
    aggregator_name="test_aggr",
    websockets_domain_name=websocket_domain_name
)

device_args = {
    "simulation_id": simulation_id,
    "domain_name": domain_name,
    "websockets_domain_name": websocket_domain_name,
    "autoregister": False,
    "start_websocket": False
}

load1_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    device_args["simulation_id"], "Load", device_args["domain_name"])
device_args["device_id"] = load1_uuid


load1 = RestDeviceClient(
    **device_args
)


load2_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    device_args["simulation_id"], "Load 2", device_args["domain_name"])
device_args["device_id"] = load2_uuid

load2 = RestDeviceClient(
    **device_args
)

pv1_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    device_args["simulation_id"], "PV", device_args["domain_name"])
device_args["device_id"] = pv1_uuid
pv1 = RestDeviceClient(
    **device_args
)

load1.select_aggregator(aggr.aggregator_uuid)
load2.select_aggregator(aggr.aggregator_uuid)
pv1.select_aggregator(aggr.aggregator_uuid)

area_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    simulation_id, "House", domain_name)

rest_market = RestMarketClient(simulation_id, area_uuid, domain_name, websocket_domain_name)
market_slot_string = today().add(minutes=60).format(DATE_TIME_FORMAT)
last_market_stats = rest_market.last_market_stats()

while not aggr.is_finished:
    sleep(0.5)