# flake8: noqa
# pylint: disable=duplicate-code

"""
Template file for markets management through the gsy-e-sdk api client using Redis.
"""
import os
from time import sleep
from gsy_e_sdk.aggregator import Aggregator
from gsy_e_sdk.rest_market import RestMarketClient
from gsy_e_sdk.utils import log_grid_fees_information
from gsy_e_sdk.utils import get_area_uuid_from_area_name_and_collaboration_id

MARKET_NAMES = ["Grid", "Community"]
ORACLE_NAME = "dso"
SLOT_LENGTH = 15  # leave as is
AUTOMATIC = True


class Oracle(Aggregator):
    """Class to represent the Grid Operator client type."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_cycle(self, market_info):
        current_market_fee = {}
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if area_dict["area_name"] in MARKET_NAMES:
                self.add_to_batch_commands.last_market_dso_stats(area_uuid)
                current_market_fee[area_dict["area_name"]] = area_dict[
                    "current_market_fee"
                ]
        next_market_fee = self._set_new_market_fee()
        log_grid_fees_information(MARKET_NAMES, current_market_fee, next_market_fee)

    def _set_new_market_fee(self):
        """Return the market fees for each market for the next time slot."""
        next_market_fee = {}
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if area_dict["area_name"] in MARKET_NAMES:
                next_market_fee[area_dict["area_name"]] = 10

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


def get_assets_name(node: dict) -> dict:
    """
    Parse the grid tree and return all registered assets
    wrapper for _get_assets_name
    """
    if node == {}:
        return {}
    reg_assets = {"Area": [], "Load": [], "PV": [], "Storage": []}
    _get_assets_name(node, reg_assets)
    return reg_assets


def _get_assets_name(node: dict, reg_assets: dict):
    """
    Parse the Collaboration / Canary Network registry
    Return a list of the Market nodes the user is registered to
    """
    if node.get("registered") is True:
        area_type = node["type"]
        reg_assets[area_type].append(node["name"])
    for child in node.get("children", []):
        _get_assets_name(child, reg_assets)


market_args = {
    "simulation_id": os.environ["API_CLIENT_SIMULATION_ID"],
    "domain_name": os.environ["API_CLIENT_DOMAIN_NAME"],
    "websockets_domain_name": os.environ["API_CLIENT_WEBSOCKET_DOMAIN_NAME"],
}

aggregator = Oracle(aggregator_name=ORACLE_NAME, **market_args)
if AUTOMATIC:
    registry = aggregator.get_configuration_registry()
    MARKET_NAMES = get_assets_name(registry)["Area"]

print()
print("Connecting to markets ...")

for i in MARKET_NAMES:
    market_uuid = get_area_uuid_from_area_name_and_collaboration_id(
        market_args["simulation_id"], i, market_args["domain_name"]
    )
    market_args["area_id"] = market_uuid
    market_registered = RestMarketClient(**market_args)
    market_registered.select_aggregator(aggregator.aggregator_uuid)
    print("----> Connected to ", i)
    sleep(0.3)

print(aggregator.device_uuid_list)

# loop to allow persistence
while not aggregator.is_finished:
    sleep(0.5)
