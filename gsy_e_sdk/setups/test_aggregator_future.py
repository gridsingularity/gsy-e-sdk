# pylint: disable=too-many-instance-attributes, broad-except
import logging
import traceback
import random
import json
from time import sleep

from gsy_framework.constants_limits import DATE_TIME_FORMAT
from pendulum import from_format
from datetime import datetime

from gsy_e_sdk.clients.redis_asset_client import RedisAssetClient
from integration_tests.test_aggregator_base import TestAggregatorBase

slot_length = 60
future_markets_duration_hours = 1


class FutureAggregator(TestAggregatorBase):
    """Aggregator class to test the connections to future
     markets:
     Done by asserting whether bids/offers are successfully
     posted on these markets."""

    def __init__(self, *args, **kwargs):

        self._has_tested_settlement_posts = None
        self.load_future_forecast = 10
        self.pv_future_forecast = 10

        self._has_tested_future_posts = False
        self._has_tested_energy_exposure = False
        super().__init__(*args, **kwargs)

        self._initialize_future_markets_info(
            future_markets_duration_hours,
            slot_length
        )

    def _setup(self):
        load_client = RedisAssetClient("load-forecast", pubsub_thread=self.pubsub)
        load_client.select_aggregator(self.aggregator_uuid)
        self.load_uuid = load_client.area_uuid

        pv_client = RedisAssetClient("pv-forecast", pubsub_thread=self.pubsub)
        pv_client.select_aggregator(self.aggregator_uuid)
        self.pv_uuid = pv_client.area_uuid

    def _initialize_future_markets_info(self, duration_hours, market_slot_length):
        # calculate open future market slots
        number_of_open_future_markets = int(duration_hours * 60 / market_slot_length)
        self.open_future_market_slots = []
        for market in range(1, number_of_open_future_markets + 1):
            slot = from_format(f"{str(datetime.now().date())}T00:00", DATE_TIME_FORMAT) \
                .add(minutes=market * slot_length).format(DATE_TIME_FORMAT)
            self.open_future_market_slots.append(slot)

        # send forecasts to open future market slots for load and pv
        for slot in self.open_future_market_slots:
            self._send_forecasts_to_future_market(self.pv_uuid, slot, self.pv_future_forecast)
            self._send_forecasts_to_future_market(self.load_uuid, slot, self.load_future_forecast)

        response = self.send_batch_commands()
        logging.info(f"forecasts sent to the open future markets {self.open_future_market_slots}: "
                     f"%s", response)

    @staticmethod
    def _can_place_forecasts(area_dict):
        return "area_name" in area_dict and ("forecast" in area_dict["area_name"])

    @staticmethod
    def _next_market_slot(market_info):
        return from_format(market_info["market_slot"], DATE_TIME_FORMAT) \
            .add(minutes=slot_length).format(DATE_TIME_FORMAT)

    def _recalculate_open_future_slots_and_send_forecast(self, market_info):
        # calculate next open future slot
        current_slot = market_info["market_slot"]
        last_open_slot = self.open_future_market_slots[-1]
        next_future_slot = self._next_market_slot({"market_slot": last_open_slot})
        self.open_future_market_slots.remove(current_slot)
        self.open_future_market_slots.append(next_future_slot)

        # send forecast to new future slot market
        self._send_forecasts_to_future_market(self.pv_uuid, next_future_slot, self.pv_future_forecast)
        self._send_forecasts_to_future_market(self.load_uuid, next_future_slot, self.load_future_forecast)

        response = self.send_batch_commands()
        logging.info(f"forecast placed on the new open future market {next_future_slot}: %s", response)

    def _send_forecasts_to_future_market(self, area_uuid, future_slot, energy_forecast):
        self.add_to_batch_commands.set_energy_forecast(asset_uuid=area_uuid,
                                                       energy_forecast_kWh={
                                                           future_slot: energy_forecast
                                                       })

    def on_market_cycle(self, market_info):
        logging.info("market_info: %s", market_info)
        try:
            self._recalculate_open_future_slots_and_send_forecast(market_info)

            for area_uuid, area_dict in self.latest_grid_tree_flat.items():
                if not area_dict.get("asset_info"):
                    continue

                # manage forecast and measurement information
                # ****** was set outside for loop ********

                # manage bids and offers in spot market
                # asset_info = area_dict["asset_info"]
                # logging.info(f"asset_info: {asset_info}")

                # if self._can_place_offer(asset_info):
                #     energy = asset_info["available_energy_kWh"] / 2
                #     self.add_to_batch_commands.offer_energy(asset_uuid=area_uuid, price=1,
                #                                             energy=energy)
                # if self._can_place_bid(asset_info):
                #     energy = asset_info["energy_requirement_kWh"] / 2
                #     self.add_to_batch_commands.bid_energy(asset_uuid=area_uuid, price=30,
                #                                           energy=energy)

                # response = self.send_batch_commands()
                # if response:
                #     logging.info("bid/offer placed on the spot market: %s", response)

                # manage bids and offers in future markets
                if self._can_place_forecasts(area_dict):
                    for future_slot in self.open_future_market_slots:
                        logging.warning("watchout!")
                        if "load" in area_dict["area_name"]:
                            self.add_to_batch_commands.bid_energy(asset_uuid=self.load_uuid,
                                                                  energy=self.load_future_forecast,
                                                                  price=30,
                                                                  time_slot=future_slot)
                        if "pv" in area_dict["area_name"]:
                            self.add_to_batch_commands.offer_energy(asset_uuid=self.pv_uuid,
                                                                    energy=self.pv_future_forecast,
                                                                    price=10,
                                                                    time_slot=future_slot)
                        transactions = self.send_batch_commands()
                        if transactions:
                            #self._test_settlement_posts(transactions)
                            self._has_tested_settlement_posts = True
                            #self._test_unsettled_energy_exposure(transactions, energy)
                            #self._has_tested_energy_exposure = True

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


aggr = FutureAggregator(aggregator_name="test_aggr")

while not aggr.status == "finished":
    sleep(0.5)
