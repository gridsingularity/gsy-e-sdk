# flake8: noqa
"""
Template file to implement Time of Use grid fees
strategy through the gsy-e-sdk api client using Redis.
"""
import os
import csv
from time import sleep
from pendulum import from_format, DateTime
from gsy_framework.constants_limits import DATE_TIME_FORMAT, TIME_FORMAT_SECONDS
from gsy_e_sdk.redis_aggregator import RedisAggregator
from gsy_e_sdk.redis_market import RedisMarketClient
from gsy_e_sdk.utils import log_grid_fees_information

module_dir = os.path.dirname(__file__)

# List of markets' names to be connected with the API
MARKET_NAMES = [
    "Grid",
    "Community",
]

ORACLE_NAME = "dso"
SLOT_LENGTH = 15  # leave as is


class Oracle(RedisAggregator):
    """Class to represent the Grid Operator client type."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_slot(self, market_info):
        current_market_fee = {}
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if area_dict["area_name"] in MARKET_NAMES:
                self.add_to_batch_commands.last_market_dso_stats(area_uuid)
                current_market_fee[area_dict["area_name"]] = area_dict[
                    "current_market_fee"
                ]
        next_market_fee = self._set_new_market_fee(market_info)
        log_grid_fees_information(MARKET_NAMES, current_market_fee, next_market_fee)

    def _set_new_market_fee(self, market_info):
        """Return the market fees for each market for the next time slot."""
        next_market_fee = {}
        market_time = from_format(market_info["market_slot"], DATE_TIME_FORMAT)
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if area_dict["area_name"] in MARKET_NAMES:
                next_market_fee[
                    area_dict["area_name"]
                ] = calculate_next_slot_market_fee(market_time, area_dict["area_name"])

                self.add_to_batch_commands.grid_fees(
                    area_uuid=area_uuid,
                    fee_cents_kwh=next_market_fee[area_dict["area_name"]],
                )
        self.execute_batch_commands()
        return next_market_fee

    def on_event_or_response(self, message):
        pass

    def on_finish(self, finish_info):
        self.is_finished = True


def read_fee_strategy():
    "Return a dictionary containing the Time of Use strategy loaded from the CSV input file."
    with open(
            os.path.join(module_dir, "resources/ToU.csv"), newline="", encoding="utf-8"
    ) as csvfile:
        csv_rows = csv.reader(csvfile, delimiter=" ", quotechar="|")
        headers = next(csv_rows)[0].split(";")
        market_indexes = {}
        planned_fee = {}
        for market_name in MARKET_NAMES:
            market_indexes.update({(market_name, headers.index(market_name))})
        for row in csv_rows:
            row = row[0].split(";")
            for market in MARKET_NAMES:
                planned_fee.update({(row[0], market): row[market_indexes[market]]})
    return planned_fee


def calculate_next_slot_market_fee(market_time: DateTime, market_name:str) -> float:
    """Return the market fee for the next time slot."""
    slot_time = market_time.add(minutes=SLOT_LENGTH).format(TIME_FORMAT_SECONDS)
    if (slot_time, market_name) in fee_strategy:
        next_fee = fee_strategy[(slot_time, market_name)]
        if not isinstance(next_fee, (int, float)):
            next_fee = float(next_fee.replace(",", "."))
    else:
        next_fee = None
    return next_fee


aggregator = Oracle(aggregator_name=ORACLE_NAME)
fee_strategy = read_fee_strategy()

print()
print("Connecting to markets ...")

for i in MARKET_NAMES:
    market_registered = RedisMarketClient(area_id=i)
    market_registered.select_aggregator(aggregator.aggregator_uuid)
    print("----> Connected to ", i)

print(aggregator.device_uuid_list)

# loop to allow persistence
while not aggregator.is_finished:
    sleep(0.5)
