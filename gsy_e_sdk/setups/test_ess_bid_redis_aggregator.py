import logging
from time import sleep
from gsy_e_sdk.redis_aggregator import RedisAggregator
from gsy_e_sdk.clients.redis_asset_client import RedisAssetClient


class AutoAggregator(RedisAggregator):
    """Aggregator that automatically reacts on market cycle events with bids and offers."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_cycle(self, market_info):
        logging.info("market_info: %s", market_info)
        batch_commands = {}

        for device_event in market_info["content"]:
            logging.info("device_event: %s", device_event)
            if "energy_to_buy" in device_event["device_info"] and \
                    device_event["device_info"]["energy_to_buy"] > 0.0:
                buy_energy = device_event["device_info"]["energy_to_buy"] / 2
                self.add_to_batch_commands.bid_energy(
                    area_uuid=device_event["area_uuid"], price=31 * buy_energy, energy=buy_energy
                    ).list_bids(area_uuid=device_event["area_uuid"])

        response = self.execute_batch_commands(batch_commands)
        logging.info("Batch command placed on the new market: %s", response)

    def on_tick(self, tick_info):
        logging.info("AGGREGATOR_TICK_INFO: %s", tick_info)

    def on_trade(self, trade_info):
        logging.info("AGGREGATOR_TRADE_INFO: %s", trade_info)

    def on_finish(self, finish_info):
        self.is_finished = True
        logging.info("AGGREGATOR_FINISH_INFO: %s", finish_info)


aggregator = AutoAggregator(
    aggregator_name="faizan_aggregator",
    autoregister=True)

sleep(10)
# aggregator.delete_aggregator(is_blocking=True)

# Connects one client to the storage device
storage = RedisAssetClient("storage", autoregister=True)

sleep(5)
selected = storage.select_aggregator(aggregator.aggregator_uuid)  # pylint: disable=invalid-name
logging.info("SELECTED: %s", selected)

while not aggregator.is_finished:
    sleep(0.5)
