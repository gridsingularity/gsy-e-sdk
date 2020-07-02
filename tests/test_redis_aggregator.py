import logging
from time import sleep
from pendulum import today
from d3a_api_client.redis_aggregator import RedisAggregator
from d3a_api_client.redis_device import RedisDeviceClient
from d3a_interface.constants_limits import DATE_TIME_FORMAT


class AutoAggregator(RedisAggregator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_cycle(self, market_info):
        logging.info(f"AGGREGATOR_MARKET_INFO: {market_info}")
        # if self.is_finished is True:
        #     return
        # if "content" not in market_info:
        #     return
        #
        batch_commands = {}

        for device_event in market_info["content"]:
            if "available_energy_kWh" in device_event["device_info"] and \
                    device_event["device_info"]["available_energy_kWh"] > 0.0:
                batch_commands[device_event["area_uuid"]] = [
                    {"type": "offer",
                     "price": 1,
                     "energy": device_event["device_info"]["available_energy_kWh"] / 2},
                    {"type": "list_offers"}]

            if "energy_requirement_kWh" in device_event["device_info"] and \
                    device_event["device_info"]["energy_requirement_kWh"] > 0.0:
                market_slot_string_1 = today().format(DATE_TIME_FORMAT)
                market_slot_string_2 = today().add(minutes=60).format(DATE_TIME_FORMAT)
                marker_list = [market_slot_string_1, market_slot_string_2]
                batch_commands[device_event["area_uuid"]] =\
                    [{"type": "bid",
                      "price": 30,
                      "energy": device_event["device_info"]["energy_requirement_kWh"] / 2},
                     {"type": "list_bids"},
                     {"type": "list_market_stats", "data": {"market_slots": marker_list}}
                     ]

        if batch_commands:
            response = self.batch_command(batch_commands)
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
    aggregator_name="faizan_aggregator",
    autoregister=True)

sleep(5)
# aggregator.delete_aggregator(is_blocking=True)


# Connects one client to the load device
load = RedisDeviceClient('load', autoregister=True)
# Connects a second client to the pv device
pv = RedisDeviceClient('pv', autoregister=True)

sleep(5)
selected = load.select_aggregator(aggregator.aggregator_uuid)
logging.info(f"SELECTED: {selected}")
sleep(5)
selected = pv.select_aggregator(aggregator.aggregator_uuid)
logging.info(f"SELECTED: {selected}")

while not aggregator.is_finished:
    sleep(0.5)
