"""
Template file to send live data to a Canary Network using Rest.
"""
import os
from time import sleep
from typing import List, Dict
from pendulum import from_format
from gsy_framework.constants_limits import DATE_TIME_FORMAT
from gsy_e_sdk.aggregator import Aggregator
from gsy_e_sdk.clients.rest_asset_client import RestAssetClient
from gsy_e_sdk.utils import get_area_uuid_from_area_name_and_collaboration_id, get_assets_name

ORACLE_NAME = "oracle"

# List of assets's names to be connected with the API
LOAD_NAMES = ["Load 1", "Load 2"]
PV_NAMES = ["PV 1", "PV 2"]
STORAGE_NAMES = []

CONNECT_TO_ALL_ASSETS = False


class Oracle(Aggregator):
    """Class that defines the behaviour of an "oracle" aggregator."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_slot(self, market_info):
        """Place a bid or an offer whenever a new market is created."""
        if self.is_finished is True:
            return
        self.send_forecasts(market_info)

    def on_tick(self, tick_info):
        pass

    def send_forecasts(self, market_info):
        """Send forecasts of the next market slot to the exchange."""
        forecast_market_slot = (
            from_format(market_info["market_slot"], DATE_TIME_FORMAT)
            .add(minutes=15)
            .format(DATE_TIME_FORMAT)
        )
        # pylint: disable=unused-variable
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            asset_info = area_dict.get("asset_info")
            if not asset_info:
                continue

            # Consumption assets
            if "energy_requirement_kWh" in asset_info:
                asset_name = area_dict["area_name"]
                globals()[f"{asset_name}"].set_energy_forecast(
                    energy_forecast_kWh={forecast_market_slot: 1.2},
                    do_not_wait=False,
                )

            # Generation assets
            if "available_energy_kWh" in asset_info:
                asset_name = area_dict["area_name"]
                globals()[f"{asset_name}"].set_energy_forecast(
                    energy_forecast_kWh={forecast_market_slot: 0.86},
                    do_not_wait=False,
                )

            self.execute_batch_commands()

    def on_event_or_response(self, message):
        pass

    def on_finish(self, finish_info):
        self.is_finished = True


aggregator = Oracle(aggregator_name=ORACLE_NAME)
simulation_id = os.environ["API_CLIENT_SIMULATION_ID"]
domain_name = os.environ["API_CLIENT_DOMAIN_NAME"]
websockets_domain_name = os.environ["API_CLIENT_WEBSOCKET_DOMAIN_NAME"]
asset_args = {}
if CONNECT_TO_ALL_ASSETS:
    registry = aggregator.get_configuration_registry()
    registered_assets = get_assets_name(registry)
    LOAD_NAMES = registered_assets["Load"]
    PV_NAMES = registered_assets["PV"]
    STORAGE_NAMES = registered_assets["Storage"]


def register_asset_list(asset_names: List, asset_params: Dict, asset_uuid_map: Dict) -> Dict:
    """Register the provided list of assets with the aggregator."""
    for asset_name in asset_names:
        print("Registered asset:", asset_name)
        uuid = get_area_uuid_from_area_name_and_collaboration_id(
            simulation_id, asset_name, domain_name
        )
        asset_params["asset_uuid"] = uuid
        asset_uuid_map[uuid] = asset_name
        globals()[f"{asset_name}"] = RestAssetClient(**asset_params)
        globals()[f"{asset_name}"].select_aggregator(aggregator.aggregator_uuid)
    return asset_uuid_map


print()
print("Registering assets ...")
asset_uuid_mapping = {}
assets_dict = {}
asset_uuid_mapping = register_asset_list(LOAD_NAMES + PV_NAMES + STORAGE_NAMES,
                                         asset_args, asset_uuid_mapping)
print()
print("Summary of assets registered:")
print()
print(asset_uuid_mapping)

# loop to allow persistence
while not aggregator.is_finished:
    sleep(0.5)
