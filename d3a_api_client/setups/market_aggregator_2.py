import logging
from time import sleep
from d3a_api_client.aggregator import Aggregator
from d3a_api_client.utils import get_area_uuid_from_area_name_and_collaboration_id, \
    get_sim_id_and_domain_names
from d3a_api_client.rest_market import RestMarketClient
logger = logging.getLogger()
logger.disabled = False
class TestMarketAggregator2(Aggregator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False
        self.fee_cents_per_kwh = 0
        self.grid_fee_count = 0
    def on_market_cycle(self, market_info):
        """
        market_info contains market_info dicts from all markets
        that are controlled by the aggregator
        """
        if self.is_finished is True:
            return
        if self.grid_fee_count < 40:
            self.fee_cents_per_kwh += 2
            self.grid_fee_count += 2
        elif self.fee_cents_per_kwh == 0:
            self.fee_cents_per_kwh += 1
        else:
            self.fee_cents_per_kwh -= 1
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if area_dict["area_name"] in market_names:
                if area_uuid is None:
                    continue
                self.add_to_batch_commands.grid_fees(
                    area_uuid=area_uuid, fee_cents_kwh=self.fee_cents_per_kwh)
        if self.commands_buffer_length > 0:
            response = self.execute_batch_commands()
    def on_tick(self, tick_info):
        logging.debug(f"Progress information on the device: {tick_info}")
    def on_trade(self, trade_info):
        logging.debug(f"Trade info: {trade_info}")
    def on_finish(self, finish_info):
        self.is_finished = True
simulation_id, domain_name, websockets_domain_name = get_sim_id_and_domain_names()
aggr = TestMarketAggregator2(
    simulation_id=simulation_id,
    domain_name=domain_name,
    aggregator_name="market_aggr2",
    websockets_domain_name=websockets_domain_name
)
market_args = {
    "simulation_id": simulation_id,
    "domain_name": domain_name,
    "websockets_domain_name": websockets_domain_name
}
market_names = ["House 2"]
house_2_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    market_args["simulation_id"], market_names[0], market_args["domain_name"])
market_args["area_id"] = house_2_uuid
house_2 = RestMarketClient(
    **market_args
)
house_2.select_aggregator(aggr.aggregator_uuid)
while not aggr.is_finished:
    sleep(0.5)
