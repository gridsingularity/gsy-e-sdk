# flake8: noqa
# pylint: disable=duplicate-code
"""
Template file for a trading strategy through the gsy-e-sdk api client
"""

import os
from time import sleep
from gsy_framework.utils import key_in_dict_and_not_none_and_greater_than_zero
from gsy_e_sdk.types import aggregator_client_type
from gsy_e_sdk.clients.rest_asset_client import RestAssetClient
from gsy_e_sdk.utils import get_area_uuid_from_area_name_and_collaboration_id

current_dir = os.path.dirname(__file__)
ORACLE_NAME = "oracle"

load_names = ["Load 1 L13", "Load 2 L21", "Load 3 L17"]
pv_names = ["PV 1 (4kW)", "PV 3 (5kW)"]
storage_names = ["Tesla Powerwall 3"]
AUTOMATIC = True


class Oracle(aggregator_client_type):
    """Class that defines the behaviour of an "oracle" aggregator."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_cycle(self, market_info):
        """Place a bid or an offer whenever a new market is created."""
        if self.is_finished is True:
            return
        self.post_bid_offer()
        self.execute_batch_commands()

    def on_tick(self, tick_info):
        pass

    def post_bid_offer(self):
        """Post a bid or an offer to the exchange."""
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                continue

            # Consumption assets
            if key_in_dict_and_not_none_and_greater_than_zero(
                area_dict["asset_info"], "energy_requirement_kWh"
            ):
                energy = area_dict["asset_info"]["energy_requirement_kWh"]
                self.add_to_batch_commands.bid_energy_rate(
                    asset_uuid=area_uuid, rate=10, energy=energy
                )

            # Generation assets
            if key_in_dict_and_not_none_and_greater_than_zero(
                area_dict["asset_info"], "available_energy_kWh"
            ):
                energy = area_dict["asset_info"]["available_energy_kWh"]
                self.add_to_batch_commands.offer_energy_rate(
                    asset_uuid=area_uuid, rate=10, energy=energy
                )

            # Storage assets
            if key_in_dict_and_not_none_and_greater_than_zero(
                area_dict["asset_info"], "energy_to_buy"
            ):
                buy_energy = area_dict["asset_info"]["energy_to_buy"]
                self.add_to_batch_commands.bid_energy_rate(
                    asset_uuid=area_uuid, rate=10, energy=buy_energy
                )

                sell_energy = area_dict["asset_info"]["energy_to_sell"]
                self.add_to_batch_commands.offer_energy_rate(
                    asset_uuid=area_uuid, rate=10, energy=sell_energy
                )

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


aggr = Oracle(aggregator_name=ORACLE_NAME)
AssetClient = RestAssetClient
simulation_id = os.environ["API_CLIENT_SIMULATION_ID"]
domain_name = os.environ["API_CLIENT_DOMAIN_NAME"]
websockets_domain_name = os.environ["API_CLIENT_WEBSOCKET_DOMAIN_NAME"]
asset_args = {"autoregister": False, "start_websocket": False}
if AUTOMATIC:
    registry = aggr.get_configuration_registry()
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
        asset = AssetClient(**asset_params)
        asset.select_aggregator(aggr.aggregator_uuid)
    return asset_uuid_map


print()
print("Registering assets ...")
asset_uuid_mapping = {}
asset_uuid_mapping = register_asset_list(load_names, asset_args, asset_uuid_mapping)
asset_uuid_mapping = register_asset_list(pv_names, asset_args, asset_uuid_mapping)
asset_uuid_mapping = register_asset_list(storage_names, asset_args, asset_uuid_mapping)


print()
print("Summary of assets registered:")
print()
print(asset_uuid_mapping)

# loop to allow persistence
while not aggr.is_finished:
    sleep(0.5)
