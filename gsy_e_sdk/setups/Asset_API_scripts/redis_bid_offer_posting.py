# flake8: noqa
"""
Template file for a trading strategy through the gsy-e-sdk api client
"""

import os
from time import sleep
from gsy_framework.utils import key_in_dict_and_not_none_and_greater_than_zero
from gsy_e_sdk.types import aggregator_client_type
from gsy_e_sdk.clients.redis_asset_client import RedisAssetClient

current_dir = os.path.dirname(__file__)
ORACLE_NAME = "oracle"

load_names = ["Load 1 L13", "Load 2 L21", "Load 3 L17"]
pv_names = ["PV 1 (4kW)", "PV 3 (5kW)"]
storage_names = ["Tesla Powerwall 3"]


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

            self.execute_batch_commands()

    def on_event_or_response(self, message):
        pass

    def on_finish(self, finish_info):
        self.is_finished = True


aggr = Oracle(aggregator_name=ORACLE_NAME)
AssetClient = RedisAssetClient
asset_args = {"autoregister": True, "pubsub_thread": aggr.pubsub}


def register_asset_list(asset_names, asset_params, asset_uuid_map):
    """Register the provided list of assets with the aggregator."""
    for asset_name in asset_names:
        print("Registered asset:", asset_name)
        asset_params["area_id"] = asset_name
        asset = AssetClient(**asset_params)
        asset_uuid_map[asset.area_uuid] = asset.area_id
        asset.select_aggregator(aggr.aggregator_uuid)
    return asset_uuid_map


print()
print("Registering assets ...")
asset_uuid_mapping = {}
asset_uuid_mapping = register_asset_list(load_names, asset_args, asset_uuid_mapping)
print()
print("Summary of assets registered:")
print()
print(asset_uuid_mapping)

# loop to allow persistence
while not aggr.is_finished:
    sleep(0.5)
