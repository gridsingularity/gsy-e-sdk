import json
import logging
import traceback

from gsy_e_sdk.redis_device import RedisDeviceClient
from integration_tests.test_aggregator_base import TestAggregatorBase


class PVAggregator(TestAggregatorBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # PV only places offers and does not have to be tested for placing bids
        self._has_tested_bids = True

    def _setup(self):
        pv = RedisDeviceClient("pv")
        pv.select_aggregator(self.aggregator_uuid)

    def on_market_cycle(self, market_info):
        logging.info("market_info: %s", market_info)
        try:

            for area_uuid, area_dict in self.latest_grid_tree_flat.items():
                if not area_dict.get("asset_info"):
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
            error_message = f"Raised exception: {ex}. Traceback: {traceback.format_exc()}"
            logging.error(error_message)
            self.errors.append(error_message)
