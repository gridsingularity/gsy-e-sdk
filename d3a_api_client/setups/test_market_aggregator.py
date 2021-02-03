import logging
from time import sleep
from d3a_api_client.aggregator import Aggregator
from d3a_api_client.utils import get_area_uuid_from_area_name_and_collaboration_id
from d3a_api_client.rest_market import RestMarketClient


class TestAggregator(Aggregator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False
        self.fee_cents_per_kwh = 0

    def on_market_cycle(self, market_info):
        """
        market_info contains market_info dicts from all markets
        that are controlled by the aggregator
        """
        logging.info(f"AGGREGATOR_MARKET_INFO: {market_info}")
        if self.is_finished is True:
            return
        if "content" not in market_info:
            return
        batch_commands = {}
        self.fee_cents_per_kwh += 1
        logging.info(f"{market_info}")
        for area_event in market_info["content"]:
            area_uuid = area_event["area_uuid"]
            if area_uuid is None:
                continue
            if area_uuid not in batch_commands:
                batch_commands[area_uuid] = []
            batch_commands[area_uuid].append({"type": "market_stats",
                                              "data": {}})
            batch_commands[area_uuid].append({"type": "grid_fees",
                                              "data": {"fee_const": self.fee_cents_per_kwh}})
        if batch_commands:
            response = self.batch_command(batch_commands)
            logging.info(f"Batch command placed on the new market: {response}")

    def on_tick(self, tick_info):
        logging.debug(f"Progress information on the device: {tick_info}")

    def on_trade(self, trade_info):
        logging.debug(f"Trade info: {trade_info}")

    def on_finish(self, finish_info):
        self.is_finished = True


simulation_id = "af779128-04a0-4af4-95a2-d8dc7c63079b"
domain_name = "http://localhost:8000"
websocket_domain_name = 'ws://localhost:8000/external-ws'

aggr = TestAggregator(
    simulation_id=simulation_id,
    domain_name=domain_name,
    aggregator_name="test_aggregator",
    websockets_domain_name=websocket_domain_name
)

market_args = {
    "simulation_id": simulation_id,
    "domain_name": domain_name,
    "websockets_domain_name": websocket_domain_name
}

house_1_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    market_args["simulation_id"], "House 1", market_args["domain_name"])
market_args["area_id"] = house_1_uuid
house_1 = RestMarketClient(
    **market_args
)

house_2_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    market_args["simulation_id"], "House 2", market_args["domain_name"])
market_args["area_id"] = house_2_uuid
house_2 = RestMarketClient(
    **market_args
)
house_1.select_aggregator(aggr.aggregator_uuid)
house_2.select_aggregator(aggr.aggregator_uuid)

while not aggr.is_finished:
    sleep(0.5)
