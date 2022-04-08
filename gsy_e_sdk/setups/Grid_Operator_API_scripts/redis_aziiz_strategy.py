# flake8: noqa
# pylint: disable=duplicate-code
"""
Template file for markets management through the gsy-e-sdk api client
"""
import os
import json
from time import sleep
from gsy_e_sdk.types import aggregator_client_type
from gsy_e_sdk.redis_market import RedisMarketClient
from gsy_e_sdk.utils import log_grid_fees_information

current_dir = os.path.dirname(__file__)

market_names = [
    "Grid",
    "Community",
]
ORACLE_NAME = "dso"
SLOT_LENGTH = 15
MOVING_AVERAGE_PEAK = True
LOOK_BACK = 4


class Oracle(aggregator_client_type):
    """Class that defines the behaviour of an "oracle" aggregator."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False
        self.import_kwh = {}
        self.export_kwh = {}
        self.balance_hist = []
        self.balance = {}
        self.dso_stats_response = {}

    def calculate_import_export_balance(self):
        """Calculate the balance (import - export) for each market."""
        self.dso_stats_response = self.execute_batch_commands()
        for market_event in self.dso_stats_response["responses"].items():
            self.import_kwh[market_event[1][0]["name"]] = market_event[1][0][
                "market_stats"
            ]["area_throughput"]["import"]
            self.export_kwh[market_event[1][0]["name"]] = market_event[1][0][
                "market_stats"
            ]["area_throughput"]["export"]
            self.balance[market_event[1][0]["name"]] = (
                self.import_kwh[market_event[1][0]["name"]]
                - self.export_kwh[market_event[1][0]["name"]]
            )
        self.balance_hist.append(self.balance)

    def set_new_market_fee(self):
        """Return the market fees for each market for the next time slot."""
        next_market_fee = {}
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if area_dict["area_name"] in market_names:
                if MOVING_AVERAGE_PEAK:
                    fees = []
                    # pylint: disable=unused-variable
                    for j,k in enumerate(self.balance_hist):
                        fees.append(k[area_dict["area_name"]])
                    max_ext_energy_kwh = abs(
                        sum(fees[-LOOK_BACK:]) / len(fees[-LOOK_BACK:])
                    )
                else:
                    max_ext_energy_kwh = max(
                        self.import_kwh[area_dict["area_name"]],
                        self.export_kwh[area_dict["area_name"]],
                    )

                individual_market_fees = fee_strategy[area_dict["area_name"]]
                # pylint: disable=unused-variable
                for j, k in enumerate(individual_market_fees):
                    if (
                        max_ext_energy_kwh
                        <= k["Import / Export threshold"]
                    ):
                        next_market_fee[
                            area_dict["area_name"]
                        ] = k["Grid fee"]
                        break
                    next_market_fee[area_dict["area_name"]] = 2000
                self.add_to_batch_commands.grid_fees(
                    area_uuid=area_uuid,
                    fee_cents_kwh=next_market_fee[area_dict["area_name"]],
                )
        return next_market_fee

    def on_market_cycle(self, market_info):
        current_market_fee = {}
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if area_dict["area_name"] in market_names:
                self.add_to_batch_commands.last_market_dso_stats(area_uuid)
                current_market_fee[area_dict["area_name"]] = area_dict[
                    "current_market_fee"
                ]
        self.calculate_import_export_balance()
        next_market_fee = self.set_new_market_fee()
        self.execute_batch_commands()
        log_grid_fees_information(market_names, current_market_fee, next_market_fee)

    def on_event_or_response(self, message):
        pass

    def on_finish(self, finish_info):
        self.is_finished = True


def read_fee_strategy():
    "Return a dictionary containing the Aziiz strategy loaded from the JSON input file."
    with open(
        os.path.join(current_dir, "resources/Aziiz.json"),
        "r",
        encoding="utf-8",
    ) as file:
        aziiz_fee = json.loads(file.read())
    return aziiz_fee


MarketClient = RedisMarketClient
aggr = Oracle(aggregator_name=ORACLE_NAME)
fee_strategy = read_fee_strategy()

print()
print("Connecting to markets ...")

for i in market_names:
    market_registered = RedisMarketClient(area_id=i)
    market_registered.select_aggregator(aggr.aggregator_uuid)
    print("----> Connected to ", i)
    sleep(0.3)

print(aggr.device_uuid_list)

# loop to allow persistence
while not aggr.is_finished:
    sleep(0.5)
