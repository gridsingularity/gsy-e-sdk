import json
import logging
from time import sleep

from d3a_interface.utils import key_in_dict_and_not_none

from d3a_api_client.redis_aggregator import RedisAggregator
from d3a_api_client.redis_device import RedisDeviceClient
from d3a_api_client.redis_market import RedisMarketClient


class BatchAggregator(RedisAggregator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.errors = 0
        self.status = "running"
        self._setup()
        self.is_active = True

    def _setup(self):
        load = RedisDeviceClient('load', autoregister=True)
        pv = RedisDeviceClient('pv', autoregister=True)

        load.select_aggregator(self.aggregator_uuid)
        pv.select_aggregator(self.aggregator_uuid)

        redis_market = RedisMarketClient('house-2')

    def on_market_cycle(self, market_info):
        logging.info(f"market_info: {market_info}")
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
                                                          energy=device_event["device_info"][
                                                                     "energy_requirement_kWh"] / 2) \
                        .list_bids(area_uuid=device_event["area_uuid"]) \
                        .last_market_stats(area_uuid=device_event["area_uuid"])

            if self.len_commands_buffer:
                if not self._client_command_buffer.buffer_length > 0:
                    self.errors += 1
                transaction = self.execute_batch_commands()
                if transaction is None:
                    self.errors += 1
                logging.info(f"Batch command placed on the new market")

    def on_finish(self, finish_info):
        self.status = "finished"
