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

TICKS = 10  # leave as is


class Oracle(aggregator_client_type):
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
        i = int(float(tick_info["slot_completion"].strip("%")) / TICKS)
        self.post_bid_offer(i)
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
                for i in range(0, TICKS):
                    if i < TICKS - 2:
                        load_strategy.append(
                            round(
                                fit_rate
                                - self.asset_strategy[area_uuid]["fee_to_market_maker"]
                                + (
                                        market_maker_rate
                                        + 2
                                        * self.asset_strategy[area_uuid][
                                            "fee_to_market_maker"
                                        ]
                                        - fit_rate
                                )
                                * (i / TICKS),
                                3,
                            )
                        )
                    else:
                        load_strategy.append(
                            round(
                                market_maker_rate
                                + self.asset_strategy[area_uuid]["fee_to_market_maker"],
                                3,
                            )
                        )
                self.asset_strategy[area_uuid]["buy_rates"] = load_strategy

            # Generation strategy
            if "available_energy_kWh" in area_dict["asset_info"]:
                gen_strategy = []
                for i in range(0, TICKS):
                    if i < TICKS - 2:
                        gen_strategy.append(
                            round(
                                max(
                                    0,
                                    market_maker_rate
                                    + self.asset_strategy[area_uuid][
                                        "fee_to_market_maker"
                                    ]
                                    - (
                                            market_maker_rate
                                            + 2
                                            * self.asset_strategy[area_uuid][
                                                "fee_to_market_maker"
                                            ]
                                            - fit_rate
                                    )
                                    * (i / TICKS),
                                ),
                                3,
                            )
                        )
                    else:
                        gen_strategy.append(
                            round(
                                max(
                                    0,
                                    fit_rate
                                    - self.asset_strategy[area_uuid][
                                        "fee_to_market_maker"
                                    ],
                                ),
                                3,
                            )
                        )
                self.asset_strategy[area_uuid]["sell_rates"] = gen_strategy

            # Storage strategy
            if "used_storage" in area_dict["asset_info"]:
                batt_buy_strategy = []
                batt_sell_strategy = []
                for i in range(0, TICKS):
                    batt_buy_strategy.append(
                        round(
                            fit_rate
                            - self.asset_strategy[area_uuid]["fee_to_market_maker"]
                            + (
                                    med_price
                                    - (
                                            fit_rate
                                            - self.asset_strategy[area_uuid][
                                                "fee_to_market_maker"
                                            ]
                                    )
                            )
                            * (i / TICKS),
                            3,
                        )
                    )
                    batt_sell_strategy.append(
                        round(
                            market_maker_rate
                            + self.asset_strategy[area_uuid]["fee_to_market_maker"]
                            - (
                                    market_maker_rate
                                    + self.asset_strategy[area_uuid]["fee_to_market_maker"]
                                    - med_price
                            )
                            * (i / TICKS),
                            3,
                        )
                    )

                self.asset_strategy[area_uuid]["buy_rates"] = batt_buy_strategy
                self.asset_strategy[area_uuid]["sell_rates"] = batt_sell_strategy

    def post_bid_offer(self, i=0):
        """Post a bid or an offer to the exchange."""
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                continue

            # Consumption assets
            if key_in_dict_and_not_none_and_greater_than_zero(
                area_dict["asset_info"], "energy_requirement_kWh"
            ):
                rate = self.asset_strategy[area_uuid]["buy_rates"][i]
                energy = area_dict["asset_info"]["energy_requirement_kWh"]
                self.add_to_batch_commands.bid_energy_rate(
                    asset_uuid=area_uuid, rate=rate, energy=energy
                )

            # Generation assets
            if key_in_dict_and_not_none_and_greater_than_zero(
                area_dict["asset_info"], "available_energy_kWh"
            ):
                rate = self.asset_strategy[area_uuid]["sell_rates"][i]
                energy = area_dict["asset_info"]["available_energy_kWh"]
                self.add_to_batch_commands.offer_energy_rate(
                    asset_uuid=area_uuid, rate=rate, energy=energy
                )

            # Storage assets
            if "energy_to_buy" in area_dict["asset_info"]:
                buy_energy = area_dict["asset_info"]["energy_to_buy"]
                if buy_energy > 0.0:
                    buy_rate = self.asset_strategy[area_uuid]["buy_rates"][i]
                    self.add_to_batch_commands.bid_energy_rate(
                        asset_uuid=area_uuid, rate=buy_rate, energy=buy_energy
                    )
                sell_energy = area_dict["asset_info"]["energy_to_sell"]
                if sell_energy > 0.0:
                    sell_energy = area_dict["asset_info"]["energy_to_sell"]
                    sell_rate = self.asset_strategy[area_uuid]["sell_rates"][i]
                    self.add_to_batch_commands.offer_energy_rate(
                        asset_uuid=area_uuid, rate=sell_rate, energy=sell_energy
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
