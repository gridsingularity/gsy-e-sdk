# flake8: noqa
# pylint: disable=duplicate-code
"""
Template file for a trading strategy through the gsy-e-sdk api client using Rest.
"""

import os
from time import sleep
from gsy_e_sdk.aggregator import Aggregator
from gsy_e_sdk.clients.rest_asset_client import RestAssetClient
from gsy_e_sdk.utils import get_area_uuid_from_area_name_and_collaboration_id

current_dir = os.path.dirname(__file__)
ORACLE_NAME = "oracle"

LOAD_NAMES = ["Load 1 L13", "Load 2 L21", "Load 3 L17"]
PV_NAMES = ["PV 1 (4kW)", "PV 3 (5kW)"]
STORAGE_NAMES = ["Tesla Powerwall 3"]
AUTOMATIC = True


class Oracle(Aggregator):
    """Class that defines the behaviour of an "oracle" aggregator."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_cycle(self, market_info):
        """Place a bid or an offer whenever a new market is created."""
        if self.is_finished is True:
            return
        self.post_bid_offer()

    def on_tick(self, tick_info):
        pass

    def post_bid_offer(self):
        """Post a bid or an offer to the exchange."""
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            asset_info = area_dict["asset_info"]
            if not asset_info:
                continue

            # Consumption assets
            required_energy = asset_info.get("energy_requirement_kWh")
            if required_energy:
                self.add_to_batch_commands.bid_energy_rate(
                    asset_uuid=area_uuid, rate=10, energy=required_energy
                )

            # Generation assets
            available_energy = asset_info.get("available_energy_kWh")
            if available_energy:
                self.add_to_batch_commands.offer_energy_rate(
                    asset_uuid=area_uuid, rate=10, energy=available_energy
                )

            # Storage assets
            buy_energy = asset_info.get("energy_to_buy")
            if buy_energy:
                self.add_to_batch_commands.bid_energy_rate(
                    asset_uuid=area_uuid, rate=10, energy=buy_energy
                )

            sell_energy = asset_info.get("energy_to_sell")
            if sell_energy:
                self.add_to_batch_commands.offer_energy_rate(
                    asset_uuid=area_uuid, rate=10, energy=sell_energy
                )

            self.execute_batch_commands()

    def on_event_or_response(self, message):
        pass

    def on_finish(self, finish_info):
        self.is_finished = True


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


aggregator = Oracle(aggregator_name=ORACLE_NAME)
simulation_id = os.environ["API_CLIENT_SIMULATION_ID"]
domain_name = os.environ["API_CLIENT_DOMAIN_NAME"]
websockets_domain_name = os.environ["API_CLIENT_WEBSOCKET_DOMAIN_NAME"]
asset_args = {"autoregister": False, "start_websocket": False}
if AUTOMATIC:
    registry = aggregator.get_configuration_registry()
    registered_assets = get_assets_name(registry)
    load_names = registered_assets["Load"]
    pv_names = registered_assets["PV"]
    storage_names = registered_assets["Storage"]


def register_asset_list(asset_names, asset_params, asset_uuid_map):
    """Register the provided list of assets with the aggregator."""
    for asset_name in asset_names:
        print("Registered asset:", asset_name)
        uuid = get_area_uuid_from_area_name_and_collaboration_id(
            simulation_id, asset_name, domain_name
        )
        asset_params["asset_uuid"] = uuid
        asset_uuid_map[uuid] = asset_name
        asset = RestAssetClient(**asset_params)
        asset.select_aggregator(aggregator.aggregator_uuid)
    return asset_uuid_map


print()
print("Registering assets ...")
asset_uuid_mapping = {}
asset_uuid_mapping = register_asset_list(LOAD_NAMES, asset_args, asset_uuid_mapping)
asset_uuid_mapping = register_asset_list(PV_NAMES, asset_args, asset_uuid_mapping)
asset_uuid_mapping = register_asset_list(STORAGE_NAMES, asset_args, asset_uuid_mapping)


print()
print("Summary of assets registered:")
print()
print(asset_uuid_mapping)

# loop to allow persistence
while not aggregator.is_finished:
    sleep(0.5)
