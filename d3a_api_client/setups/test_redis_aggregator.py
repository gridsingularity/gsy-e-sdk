import logging
from time import sleep

from d3a_interface.utils import key_in_dict_and_not_none_and_greater_than_zero

from d3a_api_client.redis_aggregator import RedisAggregator
from d3a_api_client.redis_device import RedisDeviceClient
from d3a_api_client.redis_market import RedisMarketClient


class AutoAggregator(RedisAggregator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_cycle(self, market_info):
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                if area_uuid == redis_market.area_uuid:
                    self.add_to_batch_commands.last_market_dso_stats(area_uuid=area_uuid). \
                        grid_fees(area_uuid=area_uuid, fee_cents_kwh=5)
            else:
                logging.info(
                    f'current_market_maker_rate: {market_info["market_maker_rate"]}')
                if key_in_dict_and_not_none_and_greater_than_zero(area_dict["asset_info"],
                                                                  "available_energy_kWh"):
                    energy = area_dict["asset_info"]["available_energy_kWh"] / 2
                    self.add_to_batch_commands.offer_energy(area_uuid=area_uuid, price=1,
                                                            energy=energy)
                if key_in_dict_and_not_none_and_greater_than_zero(area_dict["asset_info"],
                                                                  "energy_requirement_kWh"):
                    energy = area_dict["asset_info"]["energy_requirement_kWh"] / 2
                    self.add_to_batch_commands.bid_energy(area_uuid=area_uuid, price=30,
                                                          energy=energy)

            response = self.execute_batch_commands()
            logging.info(f"Batch command placed on the new market: {response}")

    def on_tick(self, tick_info):
        logging.debug(f"AGGREGATOR_TICK_INFO: {tick_info}")

    def on_trade(self, trade_info):
        logging.debug(f"AGGREGATOR_TRADE_INFO: {trade_info}")

    def on_finish(self, finish_info):
        self.is_finished = True
        logging.debug(f"AGGREGATOR_FINISH_INFO: {finish_info}")

    def on_batch_response(self, market_stats):
        logging.debug(f"AGGREGATORS_BATCH_RESPONSE: {market_stats}")


aggregator = AutoAggregator(aggregator_name="test_aggr")

load = RedisDeviceClient('load', autoregister=True, pubsub_thread=aggregator.pubsub)
load.select_aggregator(aggregator.aggregator_uuid)

pv = RedisDeviceClient('pv', autoregister=True, pubsub_thread=aggregator.pubsub)
pv.select_aggregator(aggregator.aggregator_uuid)

redis_market = RedisMarketClient('house-2')
redis_market.select_aggregator(aggregator.aggregator_uuid)

while not aggregator.is_finished:
    sleep(0.5)
