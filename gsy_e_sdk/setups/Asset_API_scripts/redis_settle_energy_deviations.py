# flake8: noqa
"""
Template file for a trading strategy through the gsy-e-sdk api client
"""

import os
from time import sleep
from pendulum import from_format
from gsy_framework.constants_limits import DATE_TIME_FORMAT
from gsy_framework.utils import key_in_dict_and_not_none_and_greater_than_zero
from gsy_e_sdk.types import aggregator_client_type
from gsy_e_sdk.clients.redis_asset_client import RedisAssetClient

current_dir = os.path.dirname(__file__)
ORACLE_NAME = "oracle"

load_names = ["H1 General Load", "H2 General Load"]
pv_names = ["H1 PV", "H2 PV"]
storage_names = []


class Oracle(aggregator_client_type):
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
        # pylint: disable=unused-variable
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                continue
            # Consumption assets
            if "energy_requirement_kWh" in area_dict["asset_info"]:

                asset_name = area_dict["area_name"]
                self.add_to_batch_commands.set_energy_forecast(
                    asset_uuid=globals()[f"{asset_name}"].area_uuid,
                    energy_forecast_kWh={forecast_market_slot: 1.2},
                )
            # Generation assets
            if "available_energy_kWh" in area_dict["asset_info"]:
                asset_name = area_dict["area_name"]
                self.add_to_batch_commands.set_energy_forecast(
                    asset_uuid=globals()[f"{asset_name}"].area_uuid,
                    energy_forecast_kWh={forecast_market_slot: 0.86},
                )

    def send_measurements(self, market_info):
        """Send measurements for the current market slot to the exchange."""
        # pylint: disable=unused-variable
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                continue
            # Consumption assets
            if "energy_requirement_kWh" in area_dict["asset_info"]:
                asset_name = area_dict["area_name"]
                self.add_to_batch_commands.set_energy_measurement(
                    asset_uuid=globals()[f"{asset_name}"].area_uuid,
                    energy_measurement_kWh={market_info["market_slot"]: 1.23},
                )
            # Generation assets
            if "available_energy_kWh" in area_dict["asset_info"]:
                asset_name = area_dict["area_name"]
                self.add_to_batch_commands.set_energy_measurement(
                    asset_uuid=globals()[f"{asset_name}"].area_uuid,
                    energy_measurement_kWh={market_info["market_slot"]: 0.87},
                )

    def settle_energy_deviations(self):
        """Post the energy deviations between forecasts
        and measurements in the Settlement market."""
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                continue
            time_slot = list(area_dict["asset_info"]["unsettled_deviation_kWh"].keys())[
                -1
            ]
            unsettled_deviation = area_dict["asset_info"][
                "unsettled_deviation_kWh"
            ].get(time_slot)
            if unsettled_deviation is None or unsettled_deviation == 0:
                pass
            elif unsettled_deviation > 0:
                self.add_to_batch_commands.bid_energy_rate(
                    asset_uuid=area_uuid,
                    rate=5,
                    energy=unsettled_deviation,
                    time_slot=time_slot,
                )
            elif unsettled_deviation < 0:
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


aggr = Oracle(aggregator_name=ORACLE_NAME)
AssetClient = RedisAssetClient
asset_args = {"autoregister": True, "pubsub_thread": aggr.pubsub}


def register_asset_list(asset_names, asset_params, asset_uuid_map):
    """Register the provided list of assets with the aggregator."""
    for asset_name in asset_names:
        print("Registered asset:", asset_name)
        asset_params["area_id"] = asset_name
        globals()[f"{asset_name}"] = AssetClient(**asset_params)
        asset_uuid_map[globals()[f"{asset_name}"].area_uuid] = globals()[
            f"{asset_name}"
        ].area_id
        globals()[f"{asset_name}"].select_aggregator(aggr.aggregator_uuid)
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
