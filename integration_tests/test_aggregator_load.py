import json
import logging
import traceback

from d3a_api_client.redis_aggregator import RedisAggregator
from d3a_api_client.redis_device import RedisDeviceClient


class LoadAggregator(RedisAggregator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.errors = 0
        self.status = "running"
        self._setup()
        self.is_active = True
        self._has_tested_bids = False

    def _setup(self):
        load = RedisDeviceClient("load")
        load.select_aggregator(self.aggregator_uuid)

    def on_market_cycle(self, market_info):
        logging.info(f"market_info: {market_info}")
        try:

            for area_uuid, area_dict in self.latest_grid_tree_flat.items():
                if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                    continue
                asset_info = area_dict["asset_info"]

                if self._can_place_bid(asset_info):
                    bid_energy = asset_info["energy_requirement_kWh"]
                    bid_price = 0.0001 * bid_energy
                    self.add_to_batch_commands.bid_energy(
                        area_uuid=area_uuid,
                        price=bid_price,
                        energy=bid_energy,
                        replace_existing=False
                    )

                transactions = self.send_batch_commands()
                if transactions:
                    bid_requests = self._filter_commands_from_responses(
                        transactions["responses"], "bid")

                    assert len(bid_requests) == 1
                    bid_response = bid_requests[0]
                    bid = json.loads(bid_response["bid"])
                    assert bid["buyer_origin"] == bid["buyer"] == "load"
                    assert bid["buyer_origin_id"] == bid["buyer_id"] == \
                           bid_response["area_uuid"]
                    assert bid["price"] == bid_price
                    assert bid["energy"] == bid_energy

                    self.add_to_batch_commands.list_bids(area_uuid)

                    transactions = self.send_batch_commands()
                    list_bid_requests = self._filter_commands_from_responses(
                        transactions["responses"], "list_bids")

                    assert len(list_bid_requests) == 1
                    bid_list = list_bid_requests[0]["bid_list"]
                    assert len(bid_list) == 1
                    assert list_bid_requests[0]["area_uuid"] == area_uuid
                    bid = bid_list[0]
                    assert bid["price"] == bid_price
                    assert bid["energy"] == bid_energy

                    self._has_tested_bids = True

        except Exception as ex:
            logging.error(f"Raised exception: {ex}. Traceback: {traceback.format_exc()}")
            self.errors += 1

    @staticmethod
    def _filter_commands_from_responses(responses, command_name):
        filtered_commands = []
        for area_uuid, response in responses.items():
            for command_dict in response:
                if command_dict["command"] == command_name:
                    filtered_commands.append(command_dict)
        return filtered_commands

    @staticmethod
    def _can_place_bid(asset_info):
        return (
            "energy_requirement_kWh" in asset_info and
            asset_info["energy_requirement_kWh"] > 0.0)

    def send_batch_commands(self):
        if self.commands_buffer_length:
            transaction = self.execute_batch_commands()
            if transaction is None:
                self.errors += 1
            else:
                for response in transaction["responses"].values():
                    for command_dict in response:
                        if command_dict["status"] == "error":
                            self.errors += 1
            logging.info(f"Batch command placed on the new market")
            return transaction

    def on_finish(self, finish_info):
        # Make sure that all test cases have been run
        if self._has_tested_bids is False:
            logging.error(
                "Not all test cases have been covered. This will be reported as failure.")
            self.errors += 1

        self.status = "finished"

