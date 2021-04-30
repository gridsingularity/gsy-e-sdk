import json
import logging
import traceback

from d3a_api_client.redis_device import RedisDeviceClient
from integration_tests.test_aggregator_base import TestAggregatorBase


class LoadAggregator(TestAggregatorBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Load only places bids and does not have to be tested for placing offers
        self._has_tested_offers = True

    def _setup(self):
        load = RedisDeviceClient("load")
        load.select_aggregator(self.aggregator_uuid)

    def on_market_cycle(self, market_info):
        logging.info(f"market_info: {market_info}")
        try:

            for area_uuid, area_dict in self.latest_grid_tree_flat.items():
                if not area_dict.get("asset_info"):
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


