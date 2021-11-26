import logging
from time import sleep

from gsy_framework.utils import key_in_dict_and_not_none_and_greater_than_zero

from gsy_e_sdk.clients.redis_asset_client import RedisAssetClient
from gsy_e_sdk.redis_aggregator import RedisAggregator
from gsy_e_sdk.redis_market import RedisMarketClient


class AutoAggregator(RedisAggregator):
    """Aggregator that automatically reacts on market cycle events with bids and offers."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_cycle(self, market_info):
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if not area_dict.get("asset_info"):
                if area_uuid == redis_market.area_uuid:
                    self.add_to_batch_commands.last_market_dso_stats(area_uuid=area_uuid). \
                        grid_fees(area_uuid=area_uuid, fee_cents_kwh=5)
            else:
                logging.info("current_market_maker_rate: %s", market_info["market_maker_rate"])
                if key_in_dict_and_not_none_and_greater_than_zero(area_dict["asset_info"],
                                                                  "available_energy_kWh"):
                    energy = area_dict["asset_info"]["available_energy_kWh"] / 2
                    self.add_to_batch_commands.offer_energy(asset_uuid=area_uuid, price=1,
                                                            energy=energy)
                if key_in_dict_and_not_none_and_greater_than_zero(area_dict["asset_info"],
                                                                  "energy_requirement_kWh"):
                    energy = area_dict["asset_info"]["energy_requirement_kWh"] / 2
                    self.add_to_batch_commands.bid_energy(asset_uuid=area_uuid, price=30,
                                                          energy=energy)

            response = self.execute_batch_commands()
            logging.info("Batch command placed on the new market: %s", response)

    def on_tick(self, tick_info):
        logging.debug("AGGREGATOR_TICK_INFO: %s", tick_info)

    def on_trade(self, trade_info):
        logging.debug("AGGREGATOR_TRADE_INFO: %s", trade_info)

    def on_finish(self, finish_info):
        self.is_finished = True
        logging.debug("AGGREGATOR_FINISH_INFO: %s", finish_info)

    def on_batch_response(self, market_stats):
        """Log the contents of a batch response when it's received."""
        logging.debug("AGGREGATORS_BATCH_RESPONSE: %s", market_stats)


aggregator = AutoAggregator(aggregator_name="test_aggr")

load = RedisAssetClient("load", autoregister=True, pubsub_thread=aggregator.pubsub)
load.select_aggregator(aggregator.aggregator_uuid)

pv = RedisAssetClient("pv", autoregister=True, pubsub_thread=aggregator.pubsub)
pv.select_aggregator(aggregator.aggregator_uuid)

redis_market = RedisMarketClient("house-2")
redis_market.select_aggregator(aggregator.aggregator_uuid)

while not aggregator.is_finished:
    sleep(0.5)
