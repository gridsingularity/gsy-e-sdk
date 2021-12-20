import json
import logging
import traceback

from gsy_framework.constants_limits import DATE_TIME_FORMAT
from pendulum import from_format

from gsy_e_sdk.clients.redis_asset_client import RedisAssetClient
from gsy_e_sdk.redis_market import RedisMarketClient
from integration_tests.test_aggregator_base import TestAggregatorBase


class BatchAggregator(TestAggregatorBase):
    """Aggregator class to test the behaviour of batch commands."""

    def __init__(self, *args, **kwargs):
        self.events_or_responses = set()
        super().__init__(*args, **kwargs)
        self.updated_house2_grid_fee_cents_kwh = 5
        self.updated_offer_bid_price = 60

    def _setup(self):
        load_asset = RedisAssetClient("load", pubsub_thread=self.pubsub)
        pv_asset = RedisAssetClient("pv", pubsub_thread=self.pubsub)
        forecast_load_asset = RedisAssetClient("forecast-measurement-load")

        load_asset.select_aggregator(self.aggregator_uuid)
        pv_asset.select_aggregator(self.aggregator_uuid)
        forecast_load_asset.select_aggregator(self.aggregator_uuid)

        self.redis_market = RedisMarketClient("house-2", pubsub_thread=self.pubsub)
        self.redis_market.select_aggregator(self.aggregator_uuid)

    @staticmethod
    def _can_place_forecast_measurements(area_dict):
        return "area_name" in area_dict and area_dict["area_name"] == "forecast-measurement-load"

    def _manage_offers(self, area_uuid, asset_info):
        if self._can_place_offer(asset_info):
            self.add_to_batch_commands.offer_energy(
                asset_uuid=area_uuid,
                price=1.1,
                energy=asset_info["available_energy_kWh"] / 4,
                replace_existing=False,
                attributes={"energy_type": "PV"}
            ).offer_energy(
                asset_uuid=area_uuid,
                price=2.2,
                energy=asset_info["available_energy_kWh"] / 4,
                replace_existing=False,
                attributes={"energy_type": "PV"}
            ).offer_energy(
                asset_uuid=area_uuid,
                price=3.3,
                energy=asset_info["available_energy_kWh"] / 4,
                replace_existing=True,
                attributes={"energy_type": "PV"}
            ).offer_energy(
                asset_uuid=area_uuid,
                price=4.4,
                energy=asset_info["available_energy_kWh"] / 4,
                replace_existing=False,
                attributes={"energy_type": "PV"}
            ).list_offers(area_uuid=area_uuid)

    def _manage_bids(self, area_uuid, asset_info):
        if self._can_place_bid(asset_info):
            self.add_to_batch_commands.bid_energy(
                asset_uuid=area_uuid,
                price=27,
                energy=asset_info["energy_requirement_kWh"] / 4,
                replace_existing=False,
                requirements=[{"price": 27 / (asset_info["energy_requirement_kWh"] / 4)}]
            ).bid_energy(
                asset_uuid=area_uuid,
                price=28,
                energy=asset_info["energy_requirement_kWh"] / 4,
                replace_existing=False,
                requirements=[{"price": 28 / (asset_info["energy_requirement_kWh"] / 4)}]
            ).bid_energy(
                asset_uuid=area_uuid,
                price=29,
                energy=asset_info["energy_requirement_kWh"] / 4,
                replace_existing=True,
                requirements=[{"price": 29 / (asset_info["energy_requirement_kWh"] / 4)}]
            ).bid_energy(
                asset_uuid=area_uuid,
                price=30,
                energy=asset_info["energy_requirement_kWh"] / 4,
                replace_existing=False,
                requirements=[{"price": 30 / (asset_info["energy_requirement_kWh"] / 4)}]
            ).list_bids(area_uuid=area_uuid)

    # pylint: disable=too-many-locals
    # pylint: disable=too-many-branches
    # pylint: disable=too-many-statements
    def on_market_cycle(self, market_info):
        logging.info("market_info: %s", market_info)
        try:  # pylint: disable=too-many-nested-blocks
            for area_uuid, area_dict in self.latest_grid_tree_flat.items():
                if area_uuid == self.redis_market.area_uuid:
                    self.add_to_batch_commands.grid_fees(
                        area_uuid=self.redis_market.area_uuid,
                        fee_cents_kwh=self.updated_house2_grid_fee_cents_kwh)
                    self.add_to_batch_commands.last_market_dso_stats(self.redis_market.area_uuid)
                if not area_dict.get("asset_info"):
                    continue
                asset_info = area_dict["asset_info"]

                self._manage_offers(area_uuid, asset_info)
                self._manage_bids(area_uuid, asset_info)

                if self._can_place_forecast_measurements(area_dict):
                    next_market_slot_str = (
                        from_format(
                            market_info["market_slot"], DATE_TIME_FORMAT).add(
                                minutes=15).format(DATE_TIME_FORMAT))
                    self.add_to_batch_commands.set_energy_forecast(
                        asset_uuid=area_uuid,
                        energy_forecast_kWh={next_market_slot_str: 1234.0}
                    ).set_energy_measurement(
                        asset_uuid=area_uuid,
                        energy_measurement_kWh={next_market_slot_str: 2345.0}
                    )

            if self.commands_buffer_length:
                transaction = self.execute_batch_commands()
                if transaction is None:
                    self.errors.append("Transaction was None after executing batch commands.")
                else:
                    for response in transaction["responses"].values():
                        for command_dict in response:
                            if command_dict["status"] == "error":
                                self.errors.append(
                                    "Error status received from response to batch commands.",
                                    f"Commands: {command_dict}")
                logging.info("Batch command placed on the new market")

                # Make assertions about the bids, if they happened during this slot
                bid_requests = self._filter_commands_from_responses(
                    transaction["responses"], "bid")
                if bid_requests:
                    # All bids in the batch have been issued
                    assert len(bid_requests) == 4
                    # All bids have been successfully received and processed
                    assert all(bid.get("status") == "ready" for bid in bid_requests)

                    list_bids_requests = self._filter_commands_from_responses(
                        transaction["responses"], "list_bids")

                    # The list_bids command has been issued once
                    assert len(list_bids_requests) == 1

                    # The bid list only contains two bids (the other two have been deleted)
                    current_bids = list_bids_requests[0]["bid_list"]
                    assert len(current_bids) == 2

                    issued_bids = [json.loads(bid_request["bid"]) for bid_request in bid_requests]

                    # The bids have been issued in the correct order
                    assert [
                               bid["original_price"] for bid in issued_bids
                           ] == [27, 28, 29, 30]

                    # The only two bids left are the last ones that have been issued
                    assert [bid["id"] for bid in current_bids] == \
                           [bid["id"] for bid in issued_bids[-2:]]

                    # The bids should maintain their requirements
                    for bid in issued_bids:
                        assert len(bid["requirements"]) == 1
                        assert "price" in bid["requirements"][0]

                    self._has_tested_bids = True

                    #  Forecasts and measurements were posted only for the load device
                    forecast_requests = self._filter_commands_from_responses(
                        transaction["responses"], "set_energy_forecast")
                    if forecast_requests:
                        assert len(forecast_requests) == 1
                        assert forecast_requests[0]["status"] == "ready"
                        assert forecast_requests[0]["command"] == "set_energy_forecast"
                        assert (
                            list(forecast_requests[0]["set_energy_forecast"][
                                "energy_forecast"].values())[0] == 1234.0)

                    measurement_requests = self._filter_commands_from_responses(
                        transaction["responses"], "set_energy_measurement")
                    if measurement_requests:
                        assert len(measurement_requests) == 1
                        assert measurement_requests[0]["status"] == "ready"
                        assert measurement_requests[0]["command"] == "set_energy_measurement"
                        assert (
                            list(measurement_requests[0]["set_energy_measurement"][
                                "energy_measurement"].values())[0] == 2345.0)

                # Make assertions about the offers, if they happened during this slot
                offer_requests = self._filter_commands_from_responses(
                    transaction["responses"], "offer")
                if offer_requests:
                    # All offers in the batch have been issued
                    assert len(offer_requests) == 4
                    # All offers have been successfully received and processed
                    assert all(offer.get("status") == "ready" for offer in offer_requests)

                    list_offers_requests = self._filter_commands_from_responses(
                        transaction["responses"], "list_offers")

                    # The list_offers command has been issued once
                    assert len(list_offers_requests) == 1

                    # The offers list only contains two offers (the other two have been deleted)
                    current_offers = list_offers_requests[0]["offer_list"]
                    assert len(current_offers) == 2

                    issued_offers = [
                        json.loads(offer_request["offer"]) for offer_request in offer_requests]

                    # The offers have been issued in the correct order
                    assert [
                               offer["original_price"] for offer in issued_offers
                           ] == [1.1, 2.2, 3.3, 4.4]

                    # The only two offers left are the last ones that have been issued
                    assert [offer["id"] for offer in current_offers] == \
                           [offer["id"] for offer in issued_offers[-2:]]

                    # The offers should maintain the attributes
                    assert all(offer["attributes"] == {"energy_type": "PV"}
                               for offer in issued_offers)
                    self._has_tested_offers = True

        except Exception as ex:
            error_message = f"Raised exception: {ex}. Traceback: {traceback.format_exc()}"
            logging.error(error_message)
            self.errors.append(error_message)

    def on_event_or_response(self, message):
        if "event" in message:
            self.events_or_responses.add(message["event"])
        if "command" in message:
            self.events_or_responses.add(message["command"])
