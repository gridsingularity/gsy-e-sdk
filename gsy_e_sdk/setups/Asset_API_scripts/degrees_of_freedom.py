# flake8: noqa
"""
Template file for a trading strategy through the gsy-e-sdk api client
"""

import os
import json
from time import sleep
from gsy_e_sdk.types import aggregator_client_type
from gsy_e_sdk.clients.redis_asset_client import RedisAssetClient

current_dir = os.path.dirname(__file__)
ORACLE_NAME = "oracle"

load_names = ["Load 1 L13", "Load 2 L21", "Load 3 L17"]
pv_names = ["PV 9 (15kW)", "PV 5 (10kW)"]


class Oracle(aggregator_client_type):
    """Class that defines the behaviour of an "oracle" aggregator."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False
        self.degrees_of_freedom = {}

    def on_market_cycle(self, market_info):
        """Place a bid or an offer whenever a new market is created."""
        if self.is_finished is True:
            return
        self.read_requirements()
        self.post_bid()
        self.execute_batch_commands()

    def on_tick(self, tick_info):
        pass

    def read_requirements(self):
        """Load the JSON file containing the list of requirements for each asset."""
        with open(
            os.path.join(current_dir, "resources/requirements.json"),
            "r",
            encoding="utf-8",
        ) as file:
            self.degrees_of_freedom = json.loads(file.read())

    def post_bid(self):
        """Post a bid to the exchange."""
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                continue
            if (
                "energy_requirement_kWh" in area_dict["asset_info"]
                and area_dict["asset_info"]["energy_requirement_kWh"] > 0.0
            ):
                rate = 10
                energy = area_dict["asset_info"]["energy_requirement_kWh"]
                if area_dict["area_name"] in self.degrees_of_freedom:
                    requirements = self.build_requirements_dict(
                        rate, energy, area_dict["area_name"]
                    )
                    self.add_to_batch_commands.bid_energy_rate(
                        asset_uuid=area_uuid,
                        rate=rate,
                        energy=energy,
                        requirements=requirements,
                        replace_existing=True,
                    )

                else:
                    self.add_to_batch_commands.bid_energy_rate(
                        asset_uuid=area_uuid, rate=rate, energy=energy
                    )

    def build_requirements_dict(self, rate, energy, area_name):
        """Return a dictionary with the requirements of the asset."""
        asset_dof_list = self.degrees_of_freedom[area_name]
        for asset_dof in asset_dof_list:
            id_trading_partners_list = get_partner_ids(asset_dof["Trading Partners"])
            energy_types_list = asset_dof["Energy Types"]
            energy_requirement = (
                energy
                if asset_dof["Energy"] == "None"
                else min(asset_dof["Energy"], energy)
            )
            rate_requirement = (
                rate if asset_dof["Rate"] == "None" else asset_dof["Rate"]
            )
            requirements = [
                {
                    "trading_partners": id_trading_partners_list,
                    "energy_type": energy_types_list,
                    "energy": energy_requirement,
                    "price": rate_requirement * energy_requirement,
                }
            ]
        return requirements

    def on_event_or_response(self, message):
        pass

    def on_finish(self, finish_info):
        self.is_finished = True


aggr = Oracle(aggregator_name=ORACLE_NAME)
AssetClient = RedisAssetClient
asset_args = {"autoregister": True, "pubsub_thread": aggr.pubsub}


def get_partner_ids(partners):
    """Get the trading partners' UUID from the assets' names."""
    asset_ids = {v: k for k, v in asset_uuid_mapping.items()}
    return [asset_ids[p] for p in partners]


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
asset_uuid_mapping = register_asset_list(pv_names, asset_args, asset_uuid_mapping)
print()
print("Summary of assets registered:")
print()
print(asset_uuid_mapping)

# loop to allow persistence
while not aggr.is_finished:
    sleep(0.5)
