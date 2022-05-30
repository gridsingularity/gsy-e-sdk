# flake8: noqa
"""
Template file for a trading strategy through the gsy-e-sdk api client using Redis.
"""

import os
from time import sleep
from gsy_e_sdk.redis_aggregator import RedisAggregator
from gsy_e_sdk.clients.redis_asset_client import RedisAssetClient

current_dir = os.path.dirname(__file__)
ORACLE_NAME = "oracle"

LOAD_NAMES = ["Load 1 L13", "Load 2 L21", "Load 3 L17"]
PV_NAMES = ["PV 1 (4kW)", "PV 3 (5kW)"]
STORAGE_NAMES = ["Tesla Powerwall 3"]


class Oracle(RedisAggregator):
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


aggregator = Oracle(aggregator_name=ORACLE_NAME)
asset_args = {"autoregister": True, "pubsub_thread": aggregator.pubsub}


def register_asset_list(asset_names, asset_params, asset_uuid_map):
    """Register the provided list of assets with the aggregator."""
    for asset_name in asset_names:
        print("Registered asset:", asset_name)
        asset_params["area_id"] = asset_name
        asset = RedisAssetClient(**asset_params)
        asset_uuid_map[asset.area_uuid] = asset.area_id
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
