import logging

import json
import traceback

from integration_tests.test_aggregator_base import TestAggregatorBase
from gsy_e_sdk.redis_device import RedisDeviceClient


class EssAggregator(TestAggregatorBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _setup(self):
        storage = RedisDeviceClient("storage")
        storage.select_aggregator(self.aggregator_uuid)

    def on_market_cycle(self, market_info):
        logging.info("market_info: %s", market_info)
        try:
            for area_uuid, area_dict in self.latest_grid_tree_flat.items():
                if not area_dict.get("asset_info"):
                    continue
                asset_info = area_dict["asset_info"]
                if self._can_place_bid(asset_info):
                    bid_energy = asset_info["energy_to_buy"]
                    bid_price = 31 * bid_energy
                    self.add_to_batch_commands.bid_energy(
                        area_uuid=area_uuid,
                        price=bid_price,
                        energy=bid_energy,
                        replace_existing=False
                    )

                if self._can_place_offer(asset_info):
                    offer_energy = asset_info["energy_to_sell"]
                    offer_price = 10 * offer_energy
                    self.add_to_batch_commands.offer_energy(
                        area_uuid=area_uuid,
                        price=offer_price,
                        energy=offer_energy,
                        replace_existing=False
                    )

            if self.commands_buffer_length:

                transactions = self.send_batch_commands()
                if transactions:
                    # Make assertions about the bids, if they happened during this slot
                    bid_requests = self._filter_commands_from_responses(
                        transactions["responses"], "bid")

                    if bid_requests:
                        assert len(bid_requests) == 1
                        bid_response = bid_requests[0]
                        bid = json.loads(bid_response["bid"])
                        assert bid_response["status"] == "ready"
                        assert bid["buyer_origin"] == bid["buyer"] == "storage"
                        assert bid["buyer_origin_id"] == bid["buyer_id"] == bid_response[
                            "area_uuid"]
                        assert bid["price"] == bid_price
                        assert bid["energy"] == bid_energy
                        self._has_tested_bids = True

                    # Make assertions about the offers, if they happened during this slot
                    offer_requests = self._filter_commands_from_responses(
                        transactions["responses"], "offer")
                    if offer_requests:
                        assert len(offer_requests) == 1
                        offer_response = offer_requests[0]
                        offer = json.loads(offer_response["offer"])
                        assert offer["seller_origin"] == offer["seller"] == "storage"
                        assert offer["seller_origin_id"] == offer["seller_id"] == \
                               offer_response["area_uuid"]
                        assert offer["price"] == offer_price
                        assert offer["energy"] == offer_energy

                        self._has_tested_offers = True
        except Exception as ex:
            error_message = f"Raised exception: {ex}. Traceback: {traceback.format_exc()}"
            logging.error(error_message)
            self.errors.append(error_message)

    @staticmethod
    def _can_place_bid(asset_info):
        return (
            "energy_to_buy" in asset_info and
            asset_info["energy_to_buy"] > 0.0)

    @staticmethod
    def _can_place_offer(asset_info):
        return (
            "energy_to_sell" in asset_info and
            asset_info["energy_to_sell"] > 0.0)
