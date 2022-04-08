# flake8: noqa
# pylint: disable=duplicate-code

"""
Template file for markets management through the gsy-e-sdk api client
"""
import os
import csv
from time import sleep
from pendulum import from_format
from gsy_framework.constants_limits import DATE_TIME_FORMAT, TIME_FORMAT_SECONDS
from gsy_e_sdk.types import aggregator_client_type
from gsy_e_sdk.rest_market import RestMarketClient
from gsy_e_sdk.utils import log_grid_fees_information
from gsy_e_sdk.utils import get_area_uuid_from_area_name_and_collaboration_id

current_dir = os.path.dirname(__file__)

market_names = [
    "Grid",
    "Community",
]
ORACLE_NAME = "dso"
SLOT_LENGTH = 15  # leave as is
AUTOMATIC = True


class Oracle(aggregator_client_type):
    """Class to represent the Grid Operator client type."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def set_new_market_fee(self, market_info):
        """Return the market fees for each market for the next time slot."""
        next_market_fee = {}
        market_time = from_format(market_info["market_slot"], DATE_TIME_FORMAT)
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if area_dict["area_name"] in market_names:
                next_market_fee[
                    area_dict["area_name"]
                ] = calculate_next_slot_market_fee(market_time, area_dict["area_name"])

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
        self.execute_batch_commands()
        next_market_fee = self.set_new_market_fee(market_info)
        self.execute_batch_commands()
        log_grid_fees_information(market_names, current_market_fee, next_market_fee)

    def on_event_or_response(self, message):
        pass

    def on_finish(self, finish_info):
        self.is_finished = True


def read_fee_strategy():
    "Return a dictionary containing the Time of Use strategy loaded from the CSV input file."
    with open(
        os.path.join(current_dir, "resources/ToU.csv"), newline="", encoding="utf-8"
    ) as csvfile:
        csv_rows = csv.reader(csvfile, delimiter=" ", quotechar="|")
        headers = next(csv_rows)[0].split(";")
        market_indexes = {}
        planned_fee = {}
        for market_name in market_names:
            market_indexes.update({(market_name, headers.index(market_name))})
        for row in csv_rows:
            row = row[0].split(";")
            for market in market_names:
                planned_fee.update({(row[0], market): row[market_indexes[market]]})
    return planned_fee


def calculate_next_slot_market_fee(market_time, market_name):
    """Return the market fee for the next time slot."""
    slot_time = (
        market_time.add(minutes=1 * SLOT_LENGTH).time().format(TIME_FORMAT_SECONDS)
    )
    if (slot_time, market_name) in fee_strategy:
        next_fee = fee_strategy[(slot_time, market_name)]
        if not isinstance(next_fee, (int, float)):
            next_fee = float(next_fee.replace(",", "."))
    else:
        next_fee = None
    return next_fee


def get_assets_name(indict: dict) -> dict:
    """
    Parse the grid tree and return all registered assets
    wrapper for _get_assets_name
    """
    if indict == {}:
        return {}
    outdict = {"Area": [], "Load": [], "PV": [], "Storage": []}
    _get_assets_name(indict, outdict)
    return outdict


def _get_assets_name(indict: dict, outdict: dict):
    """
    Parse the Collaboration / Canary Network registry
    Return a list of the Market nodes the user is registered to
    """
    for key, value in indict.items():
        if key == "name":
            name = value
        if key == "type":
            area_type = value
        if key == "registered" and value:
            outdict[area_type].append(name)
        if "children" in key:
            for children in indict[key]:
                _get_assets_name(children, outdict)


MarketClient = RestMarketClient
market_args = {
    "simulation_id": os.environ["API_CLIENT_SIMULATION_ID"],
    "domain_name": os.environ["API_CLIENT_DOMAIN_NAME"],
    "websockets_domain_name": os.environ["API_CLIENT_WEBSOCKET_DOMAIN_NAME"],
}

aggr = Oracle(aggregator_name=ORACLE_NAME, **market_args)
if AUTOMATIC:
    registry = aggr.get_configuration_registry()
    market_names = get_assets_name(registry)["Area"]

fee_strategy = read_fee_strategy()

print()
print("Connecting to markets ...")

for i in market_names:
    market_uuid = get_area_uuid_from_area_name_and_collaboration_id(
        market_args["simulation_id"], i, market_args["domain_name"]
    )
    market_args["area_id"] = market_uuid
    market_registered = RestMarketClient(**market_args)
    market_registered.select_aggregator(aggr.aggregator_uuid)
    print("----> Connected to ", i)
    sleep(0.3)

print(aggr.device_uuid_list)

# loop to allow persistence
while not aggr.is_finished:
    sleep(0.5)
