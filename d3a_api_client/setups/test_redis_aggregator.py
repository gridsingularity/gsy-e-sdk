import logging
import json
from time import sleep
from d3a_api_client.redis_aggregator import RedisAggregator
from d3a_api_client.redis_device import RedisDeviceClient
from d3a_interface.utils import key_in_dict_and_not_none
from d3a_api_client.redis_market import RedisMarketClient


class AutoAggregator(RedisAggregator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_cycle(self, market_info):
        logging.info(f"AGGREGATOR_MARKET_INFO: {market_info}")

        for device_event in market_info["content"]:
            if "device_info" not in device_event or device_event["device_info"] is None:
                continue
            if key_in_dict_and_not_none(device_event, "grid_stats_tree"):
                json_grid_tree = json.dumps(device_event["grid_stats_tree"], indent=2)
                logging.warning(json_grid_tree)
            if "available_energy_kWh" in device_event["device_info"] and \
                    device_event["device_info"]["available_energy_kWh"] > 0.0:
                self.add_to_batch_commands.offer_energy(area_uuid=device_event["area_uuid"], price=1,
                                                        energy=device_event["device_info"]["available_energy_kWh"] / 2) \
                    .list_offers(area_uuid=device_event["area_uuid"])
            if "energy_requirement_kWh" in device_event["device_info"] and \
                    device_event["device_info"]["energy_requirement_kWh"] > 0.0:
                self.add_to_batch_commands.bid_energy(area_uuid=device_event["area_uuid"], price=30,
                                                      energy=device_event["device_info"]["energy_requirement_kWh"] / 2) \
                    .list_bids(area_uuid=device_event["area_uuid"]) \
                    .last_market_stats(area_uuid=device_event["area_uuid"])
        response = self.execute_batch_commands()
        logging.info(f"Batch command placed on the new market: {response}")

    def on_tick(self, tick_info):
        logging.info(f"AGGREGATOR_TICK_INFO: {tick_info}")

    def on_trade(self, trade_info):
        logging.info(f"AGGREGATOR_TRADE_INFO: {trade_info}")

    def on_finish(self, finish_info):
        self.is_finished = True
        logging.info(f"AGGREGATOR_FINISH_INFO: {finish_info}")

    def on_batch_response(self, market_stats):
        logging.info(f"AGGREGATORS_BATCH_RESPONSE: {market_stats}")


aggregator = AutoAggregator(
    aggregator_name="faizan_aggregator"
)

# aggregator.delete_aggregator(is_blocking=True)


# Connects one client to the load device
load = RedisDeviceClient('load', autoregister=True)
# Connects a second client to the pv device
pv = RedisDeviceClient('pv', autoregister=True)

selected = load.select_aggregator(aggregator.aggregator_uuid)
selected = load.select_aggregator(aggregator.aggregator_uuid)
logging.info(f"SELECTED: {selected}")

selected = pv.select_aggregator(aggregator.aggregator_uuid)
logging.info(f"SELECTED: {selected}")

redis_market = RedisMarketClient('house-2')
last_market_stats_results = redis_market.last_market_stats()

while not aggregator.is_finished:
    sleep(0.5)
