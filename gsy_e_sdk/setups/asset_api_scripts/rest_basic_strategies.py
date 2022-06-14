# flake8: noqa
# pylint: disable=duplicate-code
"""
Template file for a trading strategy through the gsy-e-sdk api client using Rest.
"""

import os
from time import sleep
from gsy_e_sdk.aggregator import Aggregator
from gsy_e_sdk.utils import get_assets_name
from gsy_e_sdk.clients.rest_asset_client import RestAssetClient
from gsy_e_sdk.utils import get_area_uuid_from_area_name_and_collaboration_id

current_dir = os.path.dirname(__file__)
ORACLE_NAME = "oracle"

# List of assets's names to be connected with the API
LOAD_NAMES = ["Load 1 L13", "Load 2 L21", "Load 3 L17"]
PV_NAMES = ["PV 1 (4kW)", "PV 3 (5kW)"]
STORAGE_NAMES = ["Tesla Powerwall 3"]

TICKS = 10  # leave as is
AUTOMATIC = True


class Oracle(Aggregator):
    """Class that defines the behaviour of an "oracle" aggregator."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False
        self.asset_strategy = {}

    def on_market_cycle(self, market_info):
        """Place a bid or an offer whenever a new market is created."""
        if self.is_finished is True:
            return
        self.build_strategies(market_info)
        self.post_bid_offer()
        self.execute_batch_commands()

    def on_tick(self, tick_info):
        """Place a bid or an offer each 10% of the market slot progression."""
        rate_index = int(float(tick_info["slot_completion"].strip("%")) / TICKS)
        self.post_bid_offer(rate_index)
        self.execute_batch_commands()

    def build_strategies(self, market_info):
        """
        Assign a simple strategy to each asset in the form of an array of length 10,
        ranging between Feed-in Tariff and Market Maker rates.
        """
        fit_rate = market_info["feed_in_tariff_rate"]
        market_maker_rate = market_info["market_maker_rate"]
        med_price = (market_maker_rate - fit_rate) / 2 + fit_rate

        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                continue
            self.asset_strategy[area_uuid] = {}
            self.asset_strategy[area_uuid]["asset_name"] = area_dict["area_name"]
            self.asset_strategy[area_uuid][
                "fee_to_market_maker"
            ] = self.calculate_grid_fee(
                area_uuid,
                self.get_uuid_from_area_name("Market Maker"),
                "current_market_fee",
            )

            # Consumption strategy
            if "energy_requirement_kWh" in area_dict["asset_info"]:
                load_strategy = []
                for tick in range(0, TICKS):
                    if tick < TICKS - 2:
                        buy_rate = (fit_rate -
                                    self.asset_strategy[area_uuid]["fee_to_market_maker"] +
                                    (market_maker_rate +
                                     2 * self.asset_strategy[area_uuid]["fee_to_market_maker"] -
                                     fit_rate) * (tick / TICKS)
                                    )
                        load_strategy.append(buy_rate)
                    else:
                        buy_rate = market_maker_rate + (
                            self.asset_strategy[area_uuid]["fee_to_market_maker"])
                        load_strategy.append(buy_rate)
                self.asset_strategy[area_uuid]["buy_rates"] = load_strategy

            # Generation strategy
            if "available_energy_kWh" in area_dict["asset_info"]:
                gen_strategy = []
                for tick in range(0, TICKS):
                    if tick < TICKS - 2:
                        sell_rate = (market_maker_rate +
                                     self.asset_strategy[area_uuid]["fee_to_market_maker"] -
                                     (market_maker_rate +
                                      2 * self.asset_strategy[area_uuid]["fee_to_market_maker"] -
                                      fit_rate) * (tick / TICKS)
                                     )
                        gen_strategy.append(max(0, sell_rate))
                    else:
                        sell_rate = fit_rate - (
                            self.asset_strategy[area_uuid]["fee_to_market_maker"])
                        gen_strategy.append(max(0, sell_rate))
                self.asset_strategy[area_uuid]["sell_rates"] = gen_strategy

            # Storage strategy
            if "used_storage" in area_dict["asset_info"]:
                batt_buy_strategy = []
                batt_sell_strategy = []
                for tick in range(0, TICKS):
                    buy_rate = (fit_rate -
                                self.asset_strategy[area_uuid]["fee_to_market_maker"] +
                                (med_price -
                                 (fit_rate -
                                  self.asset_strategy[area_uuid]["fee_to_market_maker"]
                                  )
                                 ) * (tick / TICKS)
                                )
                    batt_buy_strategy.append(buy_rate)
                    sell_rate = (market_maker_rate +
                                 self.asset_strategy[area_uuid]["fee_to_market_maker"] -
                                 (market_maker_rate +
                                  self.asset_strategy[area_uuid]["fee_to_market_maker"] -
                                  med_price) * (tick / TICKS)
                                 )
                    batt_sell_strategy.append(sell_rate)
                self.asset_strategy[area_uuid]["buy_rates"] = batt_buy_strategy
                self.asset_strategy[area_uuid]["sell_rates"] = batt_sell_strategy

    def post_bid_offer(self, rate_index=0):
        """Post a bid or an offer to the exchange."""
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            asset_info = area_dict.get("asset_info")
            if not asset_info:
                continue

            # Consumption assets
            required_energy = asset_info.get("energy_requirement_kWh")
            if required_energy:
                rate = self.asset_strategy[area_uuid]["buy_rates"][rate_index]
                self.add_to_batch_commands.bid_energy_rate(
                    asset_uuid=area_uuid, rate=rate, energy=required_energy
                )

            # Generation assets
            available_energy = asset_info.get("available_energy_kWh")
            if available_energy:
                rate = self.asset_strategy[area_uuid]["sell_rates"][rate_index]
                self.add_to_batch_commands.offer_energy_rate(
                    asset_uuid=area_uuid, rate=rate, energy=available_energy
                )

            # Storage assets
            buy_energy = asset_info.get("energy_to_buy")
            if buy_energy:
                buy_rate = self.asset_strategy[area_uuid]["buy_rates"][rate_index]
                self.add_to_batch_commands.bid_energy_rate(
                    asset_uuid=area_uuid, rate=buy_rate, energy=buy_energy
                )

            sell_energy = asset_info.get("energy_to_sell")
            if sell_energy:
                sell_rate = self.asset_strategy[area_uuid]["sell_rates"][rate_index]
                self.add_to_batch_commands.offer_energy_rate(
                    asset_uuid=area_uuid, rate=sell_rate, energy=sell_energy
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
asset_args = {"autoregister": False, "start_websocket": False}
if AUTOMATIC:
    registry = aggregator.get_configuration_registry()
    registered_assets = get_assets_name(registry)
    LOAD_NAMES = registered_assets["Load"]
    PV_NAMES = registered_assets["PV"]
    STORAGE_NAMES = registered_assets["Storage"]


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
