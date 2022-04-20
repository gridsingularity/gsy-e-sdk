# flake8: noqa
"""
Template file for a trading strategy through the gsy-e-sdk api client
"""
import os
from time import sleep
from pendulum import from_format
from gsy_framework.constants_limits import DATE_TIME_FORMAT
from gsy_e_sdk.types import aggregator_client_type
from gsy_e_sdk.clients.rest_asset_client import RestAssetClient
from gsy_e_sdk.utils import get_area_uuid_from_area_name_and_collaboration_id

current_dir = os.path.dirname(__file__)
ORACLE_NAME = "oracle"

load_names = ["Load 1", "Load 2"]
pv_names = ["PV 1", "PV 2"]
storage_names = []
AUTOMATIC = False


class Oracle(aggregator_client_type):
    """Class that defines the behaviour of an "oracle" aggregator."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False
        self.base_forecast_load = 10
        self.base_forecast_pv = 5

    def on_market_cycle(self, market_info):
        """Place a bid or an offer whenever a new market is created."""
        if self.is_finished is True:
            return
        self.send_live_forecasts(market_info)
        self.execute_batch_commands()

    def on_tick(self, tick_info):
        pass

    def send_live_forecasts(self, market_info):
        """Send live forecasts to the exchange."""
        for asset_name, asset in assets_dict.items():
            forecast_market_slot = (
                from_format(market_info["market_slot"], DATE_TIME_FORMAT)
                .add(minutes=15)
                .format(DATE_TIME_FORMAT)
            )
            if "Load" in asset_name:
                energy_forecast = self.base_forecast_load
            if "PV" in asset_name:
                energy_forecast = self.base_forecast_pv
            asset.set_energy_forecast(
                energy_forecast_kWh={forecast_market_slot: energy_forecast},
                do_not_wait=False,
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
# asset_args = {"autoregister": False, "start_websocket": False}
asset_args = {}
if AUTOMATIC:
    registry = aggr.get_configuration_registry()
    registered_assets = get_assets_name(registry)
    load_names = registered_assets["Load"]
    pv_names = registered_assets["PV"]
    storage_names = registered_assets["Storage"]


def register_asset_list(asset_names, asset_params, asset_uuid_map, asset_dict):
    """Register the provided list of assets with the aggregator."""
    for asset_name in asset_names:
        uuid = get_area_uuid_from_area_name_and_collaboration_id(
            simulation_id, asset_name, domain_name
        )
        asset_params["asset_uuid"] = uuid
        asset_uuid_map[uuid] = asset_name
        asset = AssetClient(**asset_params)
        asset.select_aggregator(aggr.aggregator_uuid)
        asset_dict[asset_name] = asset
    return asset_uuid_map, asset_dict


print()
print("Registering assets ...")
asset_uuid_mapping = {}
assets_dict = {}
asset_uuid_mapping, assets_dict = register_asset_list(
    load_names, asset_args, asset_uuid_mapping, assets_dict
)
asset_uuid_mapping, assets_dict = register_asset_list(
    pv_names, asset_args, asset_uuid_mapping, assets_dict
)
asset_uuid_mapping, assets_dict = register_asset_list(
    storage_names, asset_args, asset_uuid_mapping, assets_dict
)


print()
print("Summary of assets registered:")
print()
print(asset_uuid_mapping)

# loop to allow persistence
while not aggr.is_finished:
    sleep(0.5)
