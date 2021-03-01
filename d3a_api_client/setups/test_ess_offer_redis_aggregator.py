import logging
from time import sleep
from d3a_api_client.redis_aggregator import RedisAggregator
from d3a_api_client.redis_device import RedisDeviceClient


class AutoAggregator(RedisAggregator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_cycle(self, market_info):
        logging.info(f"market_info: {market_info}")

        for device_event in market_info["content"]:
            if device_event["asset_info"] and "energy_to_sell" in device_event["asset_info"] and \
                     device_event["asset_info"] and \
                    device_event["asset_info"]["energy_to_sell"] > 0.0:
                sell_energy = device_event["asset_info"]["energy_to_sell"] / 2
                self.add_to_batch_commands.offer_energy(device_event["area_uuid"],
                                                        price=15 * sell_energy,
                                                        energy=sell_energy)
                self.add_to_batch_commands.list_offers(device_event["area_uuid"])

            logging.info(f"Batch command placed on the new market")
            self.execute_batch_commands()

    def on_tick(self, tick_info):
        logging.info(f"AGGREGATOR_TICK_INFO: {tick_info}")

    def on_trade(self, trade_info):
        logging.info(f"AGGREGATOR_TRADE_INFO: {trade_info}")

    def on_finish(self, finish_info):
        self.is_finished = True
        logging.info(f"AGGREGATOR_FINISH_INFO: {finish_info}")


aggregator = AutoAggregator(
    aggregator_name="faizan_aggregator",
    )

sleep(10)
# aggregator.delete_aggregator(is_blocking=True)


# Connects one client to the storage device
storage = RedisDeviceClient('storage', autoregister=True)

sleep(5)
selected = storage.select_aggregator(aggregator.aggregator_uuid)
logging.info(f"SELECTED: {selected}")

while not aggregator.is_finished:
    sleep(0.5)
