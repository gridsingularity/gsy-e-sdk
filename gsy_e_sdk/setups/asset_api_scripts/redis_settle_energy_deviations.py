"""
Template file to post energy deviations as bids
and offers in the Settlement Market with Redis.
"""

from time import sleep
from typing import List, Dict
from pendulum import from_format
from gsy_framework.constants_limits import DATE_TIME_FORMAT
from gsy_e_sdk.redis_aggregator import RedisAggregator
from gsy_e_sdk.clients.redis_asset_client import RedisAssetClient

ORACLE_NAME = "oracle"

# List of assets' names to be connected with the API
LOAD_NAMES = ["H1 General Load", "H2 General Load"]
PV_NAMES = ["H1 PV", "H2 PV"]
STORAGE_NAMES = []


class Oracle(RedisAggregator):
    """Class that defines the behaviour of an "oracle" aggregator."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_cycle(self, market_info):
        """Place a bid or an offer whenever a new market is created."""
        if self.is_finished is True:
            return
        self.send_forecasts(market_info)
        self.send_measurements(market_info)
        self.settle_energy_deviations()
        self.post_bid_offer()
        self.execute_batch_commands()

    def send_forecasts(self, market_info):
        """Send forecasts of the next market slot to the exchange."""
        forecast_market_slot = (
            from_format(market_info["market_slot"], DATE_TIME_FORMAT)
            .add(minutes=15)
            .format(DATE_TIME_FORMAT)
        )
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            asset_info = area_dict.get("asset_info")
            if not asset_info:
                continue

            # Consumption assets
            required_energy = asset_info.get("energy_requirement_kWh")
            if required_energy:
                self.add_to_batch_commands.set_energy_forecast(
                    asset_uuid=area_uuid,
                    energy_forecast_kWh={forecast_market_slot: 1.2},
                )

            # Generation assets
            available_energy = asset_info.get("available_energy_kWh")
            if available_energy:
                self.add_to_batch_commands.set_energy_forecast(
                    asset_uuid=area_uuid,
                    energy_forecast_kWh={forecast_market_slot: 0.86},
                )

    def send_measurements(self, market_info):
        """Send measurements for the current market slot to the exchange."""
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            asset_info = area_dict.get("asset_info")
            if not asset_info:
                continue

            # Consumption assets
            required_energy = asset_info.get("energy_requirement_kWh")
            if required_energy:
                self.add_to_batch_commands.set_energy_measurement(
                    asset_uuid=area_uuid,
                    energy_measurement_kWh={market_info["market_slot"]: 1.23},
                )

            # Generation assets
            available_energy = asset_info.get("available_energy_kWh")
            if available_energy:
                self.add_to_batch_commands.set_energy_measurement(
                    asset_uuid=area_uuid,
                    energy_measurement_kWh={market_info["market_slot"]: 0.87},
                )

    def settle_energy_deviations(self):
        """Post the energy deviations between forecasts
        and measurements in the Settlement market."""
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            asset_info = area_dict.get("asset_info")
            if not asset_info:
                continue
            time_slot = (
                list(asset_info["unsettled_deviation_kWh"].keys())[-1])
            unsettled_deviation = area_dict["asset_info"][
                "unsettled_deviation_kWh"
            ].get(time_slot)
            if unsettled_deviation > 0:
                self.add_to_batch_commands.bid_energy_rate(
                    asset_uuid=area_uuid,
                    rate=5,
                    energy=unsettled_deviation,
                    time_slot=time_slot,
                )
            if unsettled_deviation < 0:
                self.add_to_batch_commands.offer_energy_rate(
                    asset_uuid=area_uuid,
                    rate=10,
                    energy=abs(unsettled_deviation),
                    time_slot=time_slot,
                )

    def on_tick(self, tick_info):
        pass

    def post_bid_offer(self):
        """Post a bid or an offer to the exchange."""
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            asset_info = area_dict.get("asset_info")
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

    def on_event_or_response(self, message):
        pass

    def on_finish(self, finish_info):
        self.is_finished = True


aggregator = Oracle(aggregator_name=ORACLE_NAME)
asset_args = {"autoregister": True, "pubsub_thread": aggregator.pubsub}


def register_asset_list(asset_names: List, asset_params: Dict, asset_uuid_map: Dict) -> Dict:
    """Register the provided list of assets with the aggregator."""
    for asset_name in asset_names:
        print("Registered asset:", asset_name)
        asset_params["area_id"] = asset_name
        globals()[f"{asset_name}"] = RedisAssetClient(**asset_params)
        asset_uuid_map[globals()[f"{asset_name}"].area_uuid] = globals()[
            f"{asset_name}"
        ].area_id
        globals()[f"{asset_name}"].select_aggregator(aggregator.aggregator_uuid)
    return asset_uuid_map


print()
print("Registering assets ...")
asset_uuid_mapping = {}
asset_uuid_mapping = register_asset_list(LOAD_NAMES + PV_NAMES + STORAGE_NAMES,
                                         asset_args, asset_uuid_mapping)
print()
print("Summary of assets registered:")
print()
print(asset_uuid_mapping)

# loop to allow persistence
while not aggregator.is_finished:
    sleep(0.5)
