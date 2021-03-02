import logging
from time import sleep
from d3a_api_client.redis_aggregator import RedisAggregator
from d3a_api_client.redis_device import RedisDeviceClient
from d3a_api_client.redis_market import RedisMarketClient
from d3a_api_client.utils import flatten_info_dict


class AutoAggregator(RedisAggregator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_cycle(self, market_info):
        for device_name, device_dict in flatten_info_dict(market_info['grid_tree']).items():

            if "asset_info" not in device_dict or device_dict["asset_info"] is None:
                continue
            if "available_energy_kWh" in device_dict["asset_info"] and \
                    device_dict["asset_info"]["available_energy_kWh"] > 0.0:
                self.add_to_batch_commands.offer_energy(area_uuid=device_dict["area_uuid"], price=1,
                                                        energy=device_dict["asset_info"]["available_energy_kWh"] / 2) \
                    .list_offers(area_uuid=device_dict["area_uuid"])
            if "energy_requirement_kWh" in device_dict["asset_info"] and \
                    device_dict["asset_info"]["energy_requirement_kWh"] > 0.0:
                self.add_to_batch_commands.bid_energy(area_uuid=device_dict["area_uuid"], price=30,
                                                      energy=device_dict["asset_info"]["energy_requirement_kWh"] / 2) \
                    .list_bids(area_uuid=device_dict["area_uuid"]) \
                    .last_market_stats(area_uuid=device_dict["area_uuid"])
        response = self.execute_batch_commands()
        logging.info(f"Batch command placed on the new market: {response}")

    def on_tick(self, tick_info):
        logging.error(f"AGGREGATOR_TICK_INFO: {tick_info}")

    def on_trade(self, trade_info):
        logging.info(f"AGGREGATOR_TRADE_INFO: {trade_info}")

    def on_finish(self, finish_info):
        self.is_finished = True
        logging.info(f"AGGREGATOR_FINISH_INFO: {finish_info}")

    def on_batch_response(self, market_stats):
        logging.info(f"AGGREGATORS_BATCH_RESPONSE: {market_stats}")


aggregator = AutoAggregator(
    aggregator_name="test_aggr"
)

aggregator.delete_aggregator(is_blocking=True)


# Connects one client to the load device
load = RedisDeviceClient('load', autoregister=True)
# Connects a second client to the pv device
pv = RedisDeviceClient('pv', autoregister=True)

selected = load.select_aggregator(aggregator.aggregator_uuid)
logging.info(f"SELECTED: {selected}")

selected = pv.select_aggregator(aggregator.aggregator_uuid)
logging.info(f"SELECTED: {selected}")

redis_market = RedisMarketClient('house-2')
redis_market.select_aggregator(aggregator.aggregator_uuid)

while not aggregator.is_finished:
    sleep(0.5)
