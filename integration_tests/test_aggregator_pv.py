import json
import logging
import traceback

from d3a_api_client.redis_aggregator import RedisAggregator
from d3a_api_client.redis_device import RedisDeviceClient


class PVAggregator(RedisAggregator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.errors = 0
        self.status = "running"
        self._setup()
        self.is_active = True
        self._has_tested_offers = False

    def _setup(self):
        pv = RedisDeviceClient("pv")
        pv.select_aggregator(self.aggregator_uuid)

    def on_market_cycle(self, market_info):
        logging.info(f"market_info: {market_info}")
        try:

            for area_uuid, area_dict in self.latest_grid_tree_flat.items():
                if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                    continue
                asset_info = area_dict["asset_info"]

                if self._can_place_offer(asset_info):
                    offer_energy = asset_info["available_energy_kWh"]
                    offer_price = 50 * offer_energy
                    self.add_to_batch_commands.offer_energy(
                        area_uuid=area_uuid,
                        price=offer_price,
                        energy=offer_energy,
                        replace_existing=False
                    )

                transactions = self.send_batch_commands()
                if transactions:
                    offer_requests = self._filter_commands_from_responses(
                        transactions["responses"], "offer")

                    assert len(offer_requests) == 1
                    offer_response = offer_requests[0]
                    offer = json.loads(offer_response["offer"])
                    assert offer["seller_origin"] == offer["seller"] == "pv"
                    assert offer["seller_origin_id"] == offer["seller_id"] == \
                           offer_response["area_uuid"]
                    assert offer["price"] == offer_price
                    assert offer["energy"] == offer_energy

                    self.add_to_batch_commands.list_offers(area_uuid)

                    transactions = self.send_batch_commands()
                    list_offer_requests = self._filter_commands_from_responses(
                        transactions["responses"], "list_offers")

                    assert len(list_offer_requests) == 1
                    offer_list = list_offer_requests[0]["offer_list"]
                    assert len(offer_list) == 1
                    assert list_offer_requests[0]["area_uuid"] == area_uuid
                    offer = offer_list[0]
                    assert offer["price"] == offer_price
                    assert offer["energy"] == offer_energy

                    self._has_tested_offers = True

        except Exception as ex:
            logging.error(f"Raised exception: {ex}. Traceback: {traceback.format_exc()}")
            self.errors += 1

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

    @staticmethod
    def _filter_commands_from_responses(responses, command_name):
        filtered_commands = []
        for area_uuid, response in responses.items():
            for command_dict in response:
                if command_dict["command"] == command_name:
                    filtered_commands.append(command_dict)
        return filtered_commands

    @staticmethod
    def _can_place_offer(asset_info):
        return (
            "available_energy_kWh" in asset_info and
            asset_info["available_energy_kWh"] > 0.0)

    def on_finish(self, finish_info):
        self.is_finished = True
        # Make sure that all test cases have been run
        if self._has_tested_offers is False:
            logging.error(
                "Not all test cases have been covered. This will be reported as failure.")
            self.errors += 1

        self.status = "finished"

