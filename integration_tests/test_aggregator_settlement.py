# pylint: disable=too-many-instance-attributes, broad-except
import logging
import traceback
import random
import json

from gsy_framework.constants_limits import DATE_TIME_FORMAT
from pendulum import from_format

from gsy_e_sdk.clients.redis_asset_client import RedisAssetClient
from integration_tests.test_aggregator_base import TestAggregatorBase


class SettlementAggregator(TestAggregatorBase):
    """Aggregator class to test the connections to settlement
     markets:
     Done by asserting whether bids/offers are successfully
     posted on these markets."""

    def __init__(self, *args, **kwargs):

        self.base_forecast_load = 10
        self.base_measurement_load = self._calculate_random_deviated_energy(
            self.base_forecast_load, 5)
        self.base_forecast_pv = 10
        self.base_measurement_pv = self._calculate_random_deviated_energy(
            self.base_forecast_pv, 5)

        self._has_tested_settlement_posts = False
        self._has_tested_energy_exposure = False
        super().__init__(*args, **kwargs)

    def _setup(self):
        load_client = RedisAssetClient("forecast-measurement-load", pubsub_thread=self.pubsub)
        load_client.select_aggregator(self.aggregator_uuid)
        self.load_uuid = load_client.area_uuid

        pv_client = RedisAssetClient("forecast-measurement-pv", pubsub_thread=self.pubsub)
        pv_client.select_aggregator(self.aggregator_uuid)
        self.pv_uuid = pv_client.area_uuid

    @staticmethod
    def _calculate_random_deviated_energy(energy, off_percentage):
        delta = energy * off_percentage / 100
        sign = 1 if random.random() < 0.5 else -1
        return energy + sign * delta

    @staticmethod
    def _can_place_forecast_measurements(area_dict):
        return "area_name" in area_dict and ("forecast-measurement" in area_dict["area_name"])

    @staticmethod
    def _next_market_slot(market_info):
        return from_format(market_info["market_slot"], DATE_TIME_FORMAT) \
            .add(minutes=60).format(DATE_TIME_FORMAT)

    def _manage_forecasts_measurements(self, area_uuid, area_dict,
                                       forecast_market_slot, current_market_slot):
        if "load" in area_dict["area_name"]:
            energy_forecast = self.base_forecast_load
            energy_measurement = self.base_measurement_load
        else:
            energy_forecast = self.base_forecast_pv
            energy_measurement = self.base_measurement_pv

        self.add_to_batch_commands.set_energy_forecast(asset_uuid=area_uuid,
                                                       energy_forecast_kWh={
                                                           forecast_market_slot: energy_forecast
                                                       }) \
            .set_energy_measurement(asset_uuid=area_uuid,
                                    energy_measurement_kWh={
                                        current_market_slot: energy_measurement
                                    })

    @staticmethod
    def _can_place_in_settlement_market(asset_info):
        return "unsettled_deviation_kWh" in asset_info and (
                asset_info["unsettled_deviation_kWh"] != 0)

    def on_market_cycle(self, market_info):
        logging.info("market_info: %s", market_info)

        try:
            for area_uuid, area_dict in self.latest_grid_tree_flat.items():
                if not area_dict.get("asset_info"):
                    continue

                # manage forecast and measurement information
                current_market_slot = market_info["market_slot"]
                forecast_market_slot = self._next_market_slot(market_info)
                if self._can_place_forecast_measurements(area_dict):
                    self._manage_forecasts_measurements(area_uuid, area_dict,
                                                        forecast_market_slot,
                                                        current_market_slot)
                    response = self.send_batch_commands()
                    logging.info("forecast/measurement placed on the new market: %s", response)

                # manage bids and offers in spot market
                asset_info = area_dict["asset_info"]
                if self._can_place_offer(asset_info):
                    energy = asset_info["available_energy_kWh"] / 2
                    self.add_to_batch_commands.offer_energy(asset_uuid=area_uuid, price=1,
                                                            energy=energy)
                if self._can_place_bid(asset_info):
                    energy = asset_info["energy_requirement_kWh"] / 2
                    self.add_to_batch_commands.bid_energy(asset_uuid=area_uuid, price=30,
                                                          energy=energy)

                response = self.send_batch_commands()
                if response:
                    logging.info("bid/offer placed on the spot market: %s", response)

                # manage bids and offers in settlement markets
                if self._can_place_in_settlement_market(asset_info):
                    for market_slot, energy in asset_info["unsettled_deviation_kWh"].items():
                        if energy and energy > 0:
                            self.add_to_batch_commands.bid_energy(asset_uuid=area_uuid,
                                                                  energy=energy, price=30,
                                                                  time_slot=market_slot)
                        elif energy and energy < 0:
                            self.add_to_batch_commands.offer_energy(asset_uuid=area_uuid,
                                                                    energy=-energy, price=1,
                                                                    time_slot=market_slot)
                        transactions = self.send_batch_commands()
                        if transactions:
                            self._test_settlement_posts(transactions)
                            self._has_tested_settlement_posts = True
                            self._test_unsettled_energy_exposure(transactions, energy)
                            self._has_tested_energy_exposure = True

        except Exception as ex:
            error_message = f"Raised exception: {ex}. Traceback: {traceback.format_exc()}"
            logging.error(error_message)
            self.errors.append(error_message)

    @staticmethod
    def _test_unsettled_energy_exposure(transactions, energy):
        for response in transactions["responses"].values():
            for command_dict in response:
                command = command_dict["command"]
                posted_energy = json.loads(command_dict[command])["energy"]
                assert posted_energy in (energy, -energy)

    @staticmethod
    def _test_settlement_posts(transactions):
        for response in transactions["responses"].values():
            for command_dict in response:
                logging.info("%s posted on the settlement markets",
                             command_dict["command"])
                assert command_dict["market_type"] == "Settlement Market"

    def on_finish(self, finish_info):
        if not self._has_tested_settlement_posts:
            error_message = (
                "Settlement connection has not been tested successfully.")
            logging.error(error_message)
            self.errors.append(error_message)

        self.status = "finished"
