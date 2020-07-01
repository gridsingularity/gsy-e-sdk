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
        # if self.is_finished is True:
        #     return
        # if "content" not in market_info:
        #     return
        #
        batch_commands = {}

        for device_event in market_info["content"]:
            if "energy_to_sell" in device_event["device_info"] and \
                    device_event["device_info"]["energy_to_sell"] > 0.0:
                sell_energy = device_event["device_info"]["energy_to_sell"] / 2
                batch_commands[device_event["area_uuid"]] = [
                    {"type": "offer",
                     "price": 15 * sell_energy,
                     "energy": sell_energy},
                    {"type": "list_offers"}]

        if batch_commands:
            self.batch_command(batch_commands)
            logging.info(f"Batch command placed on the new market")


aggregator = AutoAggregator(
    aggregator_name="faizan_aggregator",
    autoregister=True)

sleep(10)
# aggregator.delete_aggregator(is_blocking=True)


# Connects one client to the storage device
storage = RedisDeviceClient('storage', autoregister=True)

sleep(5)
selected = storage.select_aggregator(aggregator.aggregator_uuid)
logging.info(f"SELECTED: {selected}")

while not aggregator.is_finished:
    sleep(0.5)
