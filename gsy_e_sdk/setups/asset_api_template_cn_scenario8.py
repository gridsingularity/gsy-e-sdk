# flake8: noqa
"""
Template file for a trading strategy through the gsy-e-sdk api client

"""
import json
import logging
import os
from time import sleep
from gsy_e_sdk.clients.redis_asset_client import RedisAssetClient
from gsy_e_sdk.clients.rest_asset_client import RestAssetClient
from gsy_e_sdk.types import aggregator_client_type
from gsy_e_sdk.utils import get_area_uuid_from_area_name_and_collaboration_id

current_dir = os.path.dirname(__file__)
print(current_dir)

################################################
# CONFIGURATIONS
################################################

# pylint: disable=fixme
# TODO update each of the below according to the assets the API will manage
AUTOMATIC = False

ORACLE_NAME = "oracle_edu_aggregator_s8"

load_names = ["Load 1521"]
pv_names = []
storage_names = []

# set market parameters
TICKS = 10  # leave as is

################################################
# ORACLE STRUCTURE
################################################

# pylint: disable=invalid-name
class Oracle(aggregator_client_type):
    """Class that defines the behaviour of an "oracle" aggregator."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False
        # initialise variables
        self.asset_strategy = {}  # dict containing information and pricing strategy for each asset
        self.load_energy_requirements = {}
        self.generation_energy_available = {}
        self.storage_soc = {}

    ################################################
    # TRIGGERS EACH MARKET CYCLE
    ################################################
    # pylint: disable=too-many-locals
    # pylint: disable=too-many-branches
    # pylint: disable=too-many-statements
    def on_market_cycle(self, market_info):
        """
        Places a bid or an offer whenever a new market is created. The amount of energy
        for the bid/offer depends on the available energy of the PV, the required
        energy of the load, or the amount of energy in the battery.
        :param market_info: Incoming message containing the newly-created market info
        :return: None
        """
        # termination conditions (leave as is)
        if self.is_finished is True:
            return

        ################################################
        # GET MARKET AND ENERGY FEATURES
        ################################################
        FiT_rate = market_info["feed_in_tariff_rate"]
        Market_Maker_rate = market_info["market_maker_rate"]
        Med_price = (Market_Maker_rate - FiT_rate) / 2 + FiT_rate

        print("Market maker rate: ", Market_Maker_rate)
        print("Feed-in Tariff: ", FiT_rate)

        # TODO do something with the arrays if needed:
        #   - load_energy_requirements
        #   - pv_energy_availabilities
        #   - battery_soc

        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                continue
            if "energy_requirement_kWh" in area_dict["asset_info"]:
                self.load_energy_requirements[area_uuid] = area_dict["asset_info"][
                    "energy_requirement_kWh"]
            if "available_energy_kWh" in area_dict["asset_info"]:
                self.generation_energy_available[area_uuid] = area_dict["asset_info"][
                    "available_energy_kWh"]
            if "used_storage" in area_dict["asset_info"]:
                self.storage_soc[area_uuid] = (
                    area_dict["asset_info"]["used_storage"]
                    / (area_dict["asset_info"]["used_storage"]
                    + area_dict["asset_info"]["free_storage"]))

        ################################################
        # SET ASSETS' STRATEGIES
        ################################################
        # TODO configure bidding / offer strategy for each tick
        # A dictionary is created to store values for each assets such as the grid fee to the
        # market maker, the buy and sell price strategies,... current strategy is a simple ramp
        # between 2 thresholds: Feed-in Tariff and Market Maker including grid fees. buy and sell
        # strategies are stored in array of length 10, which are posted in TICKS 0 through 9 the
        # default is that each asset type you control has the same strategy, adapted with the
        # grid fees.
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                continue

            self.asset_strategy[area_uuid] = {}
            self.asset_strategy[area_uuid]["asset_name"] = area_dict["area_name"]
            # TODO change "Market Maker" with the name defined in the grid config, if needed
            self.asset_strategy[area_uuid]["fee_to_market_maker"] = self.calculate_grid_fee(
                area_uuid, self.get_uuid_from_area_name("Grid Market"), "current_market_fee")

            # Load strategy
            if "energy_requirement_kWh" in area_dict["asset_info"]:
                load_strategy = []
                for i in range(0, TICKS):
                    if i < TICKS - 2:
                        load_strategy.append(round(
                            FiT_rate - self.asset_strategy[area_uuid]["fee_to_market_maker"] +
                            (Market_Maker_rate + 2*self.asset_strategy[area_uuid][
                                "fee_to_market_maker"] - FiT_rate) * (i / TICKS), 3))
                    else:
                        load_strategy.append(round(
                            Market_Maker_rate + self.asset_strategy[area_uuid][
                                "fee_to_market_maker"], 3))
                self.asset_strategy[area_uuid]["buy_rates"] = load_strategy

            # Generation strategy
            if "available_energy_kWh" in area_dict["asset_info"]:
                gen_strategy = []
                for i in range(0, TICKS):
                    if i < TICKS - 2:
                        gen_strategy.append(round(max(
                            0,
                            Market_Maker_rate + self.asset_strategy[area_uuid][
                                "fee_to_market_maker"] -
                            (Market_Maker_rate + 2*self.asset_strategy[area_uuid][
                                "fee_to_market_maker"] - FiT_rate) * (i / TICKS)), 3))
                    else:
                        gen_strategy.append(round(max(
                            0,
                            FiT_rate - self.asset_strategy[area_uuid]["fee_to_market_maker"]), 3))
                self.asset_strategy[area_uuid]["sell_rates"] = gen_strategy

            # Storage strategy
            if "used_storage" in area_dict["asset_info"]:
                batt_buy_strategy = []
                batt_sell_strategy = []
                for i in range(0, TICKS):
                    batt_buy_strategy.append(round(
                        FiT_rate - self.asset_strategy[area_uuid][
                            "fee_to_market_maker"] + (Med_price - (FiT_rate - self.asset_strategy[
                                area_uuid]["fee_to_market_maker"])) * (i / TICKS), 3))

                    batt_sell_strategy.append(round(
                        Market_Maker_rate+self.asset_strategy[area_uuid][
                            "fee_to_market_maker"] - (Market_Maker_rate + self.asset_strategy[
                                area_uuid]["fee_to_market_maker"] - Med_price) * (i / TICKS), 3))

                self.asset_strategy[area_uuid]["buy_rates"] = batt_buy_strategy
                self.asset_strategy[area_uuid]["sell_rates"] = batt_sell_strategy

        ################################################
        # POST INITIAL BIDS AND OFFERS FOR MARKET SLOT
        ################################################
        # takes the first element in each asset strategy to post the first bids and offers
        # all bids and offers are aggregated in a single batch and then executed
        # TODO how would you self-balance your managed energy assets?
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                continue

            # Load strategy
            if (
                    "energy_requirement_kWh" in area_dict["asset_info"]
                    and area_dict["asset_info"]["energy_requirement_kWh"] > 0.0):
                rate = self.asset_strategy[area_uuid]["buy_rates"][0]
                energy = area_dict["asset_info"]["energy_requirement_kWh"]
                self.add_to_batch_commands.bid_energy_rate(
                    asset_uuid=area_uuid, rate=rate, energy=energy)

            # Generation strategy
            if (
                    "available_energy_kWh" in area_dict["asset_info"]
                    and area_dict["asset_info"]["available_energy_kWh"] > 0.0):
                rate = self.asset_strategy[area_uuid]["sell_rates"][0]
                energy = area_dict["asset_info"]["available_energy_kWh"]
                self.add_to_batch_commands.offer_energy_rate(
                    asset_uuid=area_uuid, rate=rate, energy=energy)

            # Battery strategy
            if "energy_to_buy" in area_dict["asset_info"]:
                buy_energy = area_dict["asset_info"]["energy_to_buy"]
                sell_energy = area_dict["asset_info"]["energy_to_sell"]
                # Battery buy strategy
                if buy_energy > 0.0:
                    buy_rate = self.asset_strategy[area_uuid]["buy_rates"][0]
                    self.add_to_batch_commands.bid_energy_rate(
                        asset_uuid=area_uuid, rate=buy_rate, energy=buy_energy)
                # Battery sell strategy
                if sell_energy > 0.0:
                    sell_rate = self.asset_strategy[area_uuid]["sell_rates"][0]
                    self.add_to_batch_commands.offer_energy_rate(
                        asset_uuid=area_uuid, rate=sell_rate, energy=sell_energy)

        self.execute_batch_commands()

    ################################################
    # TRIGGERS EACH TICK
    ################################################
    # Places a bid or an offer 10% of the market slot progression. The amount of energy
    # for the bid/offer depends on the available energy of the PV, the required
    # energy of the load, or the amount of energy in the battery and the energy already traded.
    def on_tick(self, tick_info):
        i = int(float(tick_info["slot_completion"].strip("%")) / TICKS)  # tick num for index

        ################################################
        # ADJUST BID AND OFFER STRATEGY (if needed)
        ################################################
        # TODO manipulate tick strategy if required
        self.asset_strategy = self.asset_strategy

        ################################################
        # UPDATE/REPLACE BIDS AND OFFERS EACH TICK
        ################################################
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():


            if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                continue

            logging.info(f"area_name= {area_dict['area_name']}")
            logging.info(f"asset_info = \n{json.dumps(area_dict['asset_info'], indent=2, sort_keys=False)}")

            # Load Strategy
            if "energy_requirement_kWh" in area_dict["asset_info"] and area_dict["asset_info"][
                "energy_requirement_kWh"] > 0.0:
                rate = self.asset_strategy[area_uuid]["buy_rates"][i]
                energy = area_dict["asset_info"]["energy_requirement_kWh"]
                self.add_to_batch_commands.bid_energy_rate(
                    asset_uuid=area_uuid, rate=rate, energy=energy)

            # Generation strategy
            if "available_energy_kWh" in area_dict["asset_info"] and area_dict["asset_info"][
                "available_energy_kWh"] > 0.0:
                rate = self.asset_strategy[area_uuid]["sell_rates"][i]
                energy = area_dict["asset_info"]["available_energy_kWh"]
                self.add_to_batch_commands.offer_energy(asset_uuid=area_uuid, price=rate*energy,
                                                        energy=energy, replace_existing=True)

            # Battery strategy
            if "energy_to_buy" in area_dict["asset_info"]:
                buy_energy = (
                    area_dict["asset_info"]["energy_to_buy"]
                    + area_dict["asset_info"]["energy_active_in_offers"])
                sell_energy = (
                    area_dict["asset_info"]["energy_to_sell"]
                    + area_dict["asset_info"]["energy_active_in_bids"])

                # Battery buy strategy
                if buy_energy > 0.0:
                    buy_rate = self.asset_strategy[area_uuid]["buy_rates"][i]
                    self.add_to_batch_commands.bid_energy_rate(
                        asset_uuid=area_uuid, rate=buy_rate, energy=buy_energy)

                # Battery sell strategy
                if sell_energy > 0.0:
                    sell_rate = self.asset_strategy[area_uuid]["sell_rates"][i]
                    self.add_to_batch_commands.offer_energy(asset_uuid=area_uuid,
                                                            price=sell_rate*sell_energy,
                                                            energy=sell_energy,
                                                            replace_existing=True)

        self.execute_batch_commands()

    ################################################
    # TRIGGERS EACH COMMAND RESPONSE AND EVENT
    ################################################
    def on_event_or_response(self, message):
        # print("message",message)
        pass

    ################################################
    # SIMULATION TERMINATION CONDITION
    ################################################
    def on_finish(self, finish_info):
        # TODO export relevant information stored during the simulation (if needed)
        self.is_finished = True


################################################
# REGISTER FOR ASSETS AND MARKETS
################################################
def get_assets_name(indict: dict) -> dict:
    """
    This function is used to parse the grid tree and returned all registered assets
    wrapper for _get_assets_name
    """
    if indict == {}:
        return {}
    outdict = {"Area": [], "Load": [], "PV": [], "Storage": []}
    _get_assets_name(indict, outdict)
    return outdict


def _get_assets_name(indict: dict, outdict: dict):
    """
    Parse the collaboration / Canary Network registry
    Returns a list of the Market, Load, PV and Storage names the user is registered to
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

if os.environ["API_CLIENT_RUN_ON_REDIS"] == "true":
    AssetClient = RedisAssetClient
    asset_args = {"autoregister": True, "pubsub_thread": aggr.pubsub}
else:
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
        if os.environ["API_CLIENT_RUN_ON_REDIS"] == "true":
            asset_params["area_id"] = asset_name
        else:
            uuid = get_area_uuid_from_area_name_and_collaboration_id(
                simulation_id, asset_name, domain_name)
            asset_params["asset_uuid"] = uuid
            asset_uuid_map[uuid] = asset_name
        asset = AssetClient(**asset_params)
        if os.environ["API_CLIENT_RUN_ON_REDIS"] == "true":
            asset_uuid_map[asset.area_uuid] = asset.area_id
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
