# flake8: noqa

"""
Template file for a trading strategy through the d3a API client
"""

import os
from time import sleep
from d3a_api_client.redis_device import RedisDeviceClient
from d3a_api_client.rest_device import RestDeviceClient
from d3a_api_client.types import aggregator_client_type
from d3a_api_client.utils import get_area_uuid_from_area_name_and_collaboration_id, get_aggregator_prefix
import logging
logger = logging.getLogger()
logger.disabled = False

current_dir = os.path.dirname(__file__)
print(current_dir)
################################################
# CONFIGURATIONS
################################################

# TODO update each of the below according to the assets the API will manage
automatic = True

oracle_name = 'oracle'

load_names = ['Load 1 L13', 'Load 2 L21', 'Load 3 L17']
pv_names = ['PV 1 (4kW)', 'PV 3 (5kW)']
storage_names = ['Tesla Powerwall 3']

# set market parameters
ticks = 10  # leave as is

################################################
# ORACLE STRUCTURE
################################################

class Oracle(aggregator_client_type):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False
        # initialise variables
        self.asset_strategy = {}  # dictionary containing information and pricing strategy for each asset
        self.load_energy_requirements = {}
        self.generation_energy_available = {}
        self.storage_soc = {}

    ################################################
    # TRIGGERS EACH MARKET CYCLE
    ################################################
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
                self.load_energy_requirements[area_uuid] = area_dict["asset_info"]["energy_requirement_kWh"]
            if "available_energy_kWh" in area_dict["asset_info"]:
                self.generation_energy_available[area_uuid] = area_dict["asset_info"]["available_energy_kWh"]
            if "used_storage" in area_dict["asset_info"]:
                self.storage_soc[area_uuid] = area_dict["asset_info"]["used_storage"] / (area_dict["asset_info"]["used_storage"] + area_dict["asset_info"]["free_storage"])

        # print(self.load_energy_requirements)
        # print(self.generation_energy_available)
        print(self.storage_soc)


        ################################################
        # SET ASSETS' STRATEGIES
        ################################################
        # TODO configure bidding / offer strategy for each tick
        # a dictionary is created to store values for each assets such as the grid fee to the market maker, the buy and sell price strategies,...
        # current strategy is a simple ramp between 2 thresholds: Feed-in Tariff and Market Maker including grid fees.
        # buy and sell strategies are stored in array of length 10, which are posted in ticks 0 through 9
        # the default is that each device type you control has the same strategy, adapted with the grid fees

        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                continue

            self.asset_strategy[area_uuid] = {}
            self.asset_strategy[area_uuid]["fee_to_market_maker"] = self.calculate_grid_fee(area_uuid, self.get_uuid_from_area_name("Market maker and FiT"), "current_market_fee")
            self.asset_strategy[area_uuid]["asset_name"] = area_dict["area_name"]
            self.asset_strategy[area_uuid]["asset_type"] = area_dict["asset_bill"]["type"]

            # Load strategy
            if 'energy_requirement_kWh' in area_dict["asset_info"]:
                load_strategy = []
                for i in range(0, ticks):
                    if i < ticks - 2:
                        load_strategy.append(round(
                            FiT_rate - self.asset_strategy[area_uuid]["fee_to_market_maker"] +
                            (Market_Maker_rate + 2*self.asset_strategy[area_uuid]["fee_to_market_maker"] - FiT_rate) * (i / ticks), 3))
                    else:
                        load_strategy.append(round(
                            Market_Maker_rate + self.asset_strategy[area_uuid]["fee_to_market_maker"], 3))
                self.asset_strategy[area_uuid]["buy_rates"] = load_strategy

            # Generation strategy
            if 'available_energy_kWh' in area_dict["asset_info"]:
                gen_strategy = []
                for i in range(0, ticks):
                    if i < ticks - 2:
                        gen_strategy.append(round(max(0, Market_Maker_rate + self.asset_strategy[area_uuid]["fee_to_market_maker"] -
                                            (Market_Maker_rate + 2*self.asset_strategy[area_uuid]["fee_to_market_maker"] - FiT_rate) * (i / ticks)), 3))
                    else:
                        gen_strategy.append(round(max(0, FiT_rate - self.asset_strategy[area_uuid]["fee_to_market_maker"]), 3))
                self.asset_strategy[area_uuid]["sell_rates"] = gen_strategy

            # Storage strategy
            if 'used_storage' in area_dict["asset_info"]:
                batt_buy_strategy = []
                batt_sell_strategy = []
                for i in range(0, ticks):
                    batt_buy_strategy.append(round(FiT_rate + (Med_price - FiT_rate) * (i / ticks), 3))
                    batt_sell_strategy.append(round(Market_Maker_rate - (Market_Maker_rate - Med_price) * (i / ticks), 3))
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

            if "energy_requirement_kWh" in area_dict["asset_info"] and area_dict["asset_info"]["energy_requirement_kWh"] > 0.0:
                rate = self.asset_strategy[area_uuid]["buy_rates"][0]
                energy = area_dict["asset_info"]["energy_requirement_kWh"]
                self.add_to_batch_commands.bid_energy_rate(area_uuid=area_uuid, rate=rate, energy=energy)

            # Generation strategy
            if "available_energy_kWh" in area_dict["asset_info"] and area_dict["asset_info"]["available_energy_kWh"] > 0.0:
                rate = self.asset_strategy[area_uuid]["sell_rates"][0]
                energy = area_dict["asset_info"]["available_energy_kWh"]
                self.add_to_batch_commands.offer_energy_rate(area_uuid=area_uuid, rate=rate, energy=energy)

            # Battery strategy
            if "energy_to_buy" in area_dict["asset_info"]:
                buy_energy = area_dict["asset_info"]["energy_to_buy"]
                sell_energy = area_dict["asset_info"]["energy_to_sell"]
                # Battery buy strategy
                if buy_energy > 0.0:
                    buy_rate = self.asset_strategy[area_uuid]["buy_rates"][0]
                    self.add_to_batch_commands.bid_energy_rate(area_uuid=area_uuid, rate=buy_rate, energy=buy_energy)
                # Battery sell strategy
                if sell_energy > 0.0:
                    sell_rate = self.asset_strategy[area_uuid]["sell_rates"][0]
                    self.add_to_batch_commands.offer_energy_rate(area_uuid=area_uuid, rate=sell_rate, energy=sell_energy)

        response_slot = self.execute_batch_commands()

    ################################################
    # TRIGGERS EACH TICK
    ################################################
    def on_tick(self, tick_info):
        i = int(float(tick_info['slot_completion'].strip('%')) / ticks)  # tick num for index

        ################################################
        # ADJUST BID AND OFFER STRATEGY (if needed)
        ################################################
        # TODO manipulate tick strategy if required
        self.asset_strategy = self.asset_strategy

        ################################################
        # UPDATE/REPLACE BIDS AND OFFERS EACH TICK (if needed)
        ################################################
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                continue

            # Load Strategy
            if "energy_requirement_kWh" in area_dict["asset_info"] and area_dict["asset_info"][
                "energy_requirement_kWh"] > 0.0:
                rate = self.asset_strategy[area_uuid]["buy_rates"][i]
                energy = area_dict["asset_info"]["energy_requirement_kWh"]
                # self.add_to_batch_commands.bid_energy_rate(area_uuid=area_uuid, rate=rate, energy=energy)
                self.add_to_batch_commands.update_bid(area_uuid=area_uuid, price=rate*energy, energy=energy)

            # Generation strategy
            if "available_energy_kWh" in area_dict["asset_info"] and area_dict["asset_info"][
                "available_energy_kWh"] > 0.0:
                rate = self.asset_strategy[area_uuid]["sell_rates"][i]
                energy = area_dict["asset_info"]["available_energy_kWh"]
                # self.add_to_batch_commands.offer_energy_rate(area_uuid=area_uuid, rate=rate, energy=energy)
                self.add_to_batch_commands.update_offer(area_uuid=area_uuid, price=rate*energy, energy=energy)

            # Battery strategy
            if "energy_to_buy" in area_dict["asset_info"]:
                buy_energy = area_dict["asset_info"]["energy_to_buy"]
                sell_energy = area_dict["asset_info"]["energy_to_sell"]
                # Battery buy strategy
                if buy_energy > 0.0:
                    buy_rate = self.asset_strategy[area_uuid]["buy_rates"][i]
                    # self.add_to_batch_commands.bid_energy_rate(area_uuid=area_uuid, rate=buy_rate, energy=buy_energy)
                    self.add_to_batch_commands.update_bid(area_uuid=area_uuid, price=buy_rate*buy_energy, energy=buy_energy)

                # Battery sell strategy
                if sell_energy > 0.0:
                    sell_rate = self.asset_strategy[area_uuid]["sell_rates"][i]
                    # self.add_to_batch_commands.offer_energy_rate(area_uuid=area_uuid, rate=sell_rate, energy=sell_energy)
                    self.add_to_batch_commands.update_offer(area_uuid=area_uuid, price=sell_rate*sell_energy, energy=sell_energy)

        response_tick = self.execute_batch_commands()

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
        self.is_finished = True


################################################
# REGISTER FOR DEVICES AND MARKETS
################################################
def get_assets_list(indict: dict) -> dict:
    """
    wrapper for _flatten_info_dict
    """
    if indict == {}:
        return {}
    outdict = {"Area": [], "Load": [], "PV": [], "Storage": []}
    _get_assets_list(indict, outdict)
    return outdict


def _get_assets_list(indict: dict, outdict: dict):
    """
    Flattens market_info/tick_info information trees
    outdict will hold references to all area subdicts of indict
    """
    for area_name, area_dict in indict.items():
        # print(indict.items())
        if area_name == "name":
            name = area_dict
        if area_name == "type":
            type = area_dict
        if area_name == "registered" and area_dict:
            outdict[type].append(name)

        # print(i)
        if 'children' in area_name:
            for area in indict[area_name]:
                # if 'children' in area:
                # print(area)
                _get_assets_list(area, outdict)


aggr = Oracle(aggregator_name=oracle_name)

if os.environ["API_CLIENT_RUN_ON_REDIS"] == "true":
    DeviceClient = RedisDeviceClient
    device_args = {"autoregister": True, "pubsub_thread": aggr.pubsub}
else:
    DeviceClient = RestDeviceClient
    simulation_id = os.environ["API_CLIENT_SIMULATION_ID"]
    domain_name = os.environ["API_CLIENT_DOMAIN_NAME"]
    websockets_domain_name = os.environ["API_CLIENT_WEBSOCKET_DOMAIN_NAME"]
    device_args = {"autoregister": False, "start_websocket": False}
    if automatic:
        registry = aggr.get_configuration_registry()
        load_names = get_assets_list(registry)["Load"]
        pv_names = get_assets_list(registry)["PV"]
        storage_names = get_assets_list(registry)["Storage"]


def register_device_list(device_names, device_args, device_uuid_map):
    for d in device_names:
        print('Registered device:', d)
        if os.environ["API_CLIENT_RUN_ON_REDIS"] == "true":
            device_args['device_id'] = d
        else:
            uuid = get_area_uuid_from_area_name_and_collaboration_id(simulation_id, d, domain_name)
            device_args['device_id'] = uuid
            device_uuid_map[uuid] = d
        device = DeviceClient(**device_args)
        if os.environ["API_CLIENT_RUN_ON_REDIS"] == "true":
            device_uuid_map[device.device_uuid] = device.area_id
        device.select_aggregator(aggr.aggregator_uuid)
    return device_uuid_map


print()
print('Registering devices ...')
device_uuid_map = {}
device_uuid_map = register_device_list(load_names, device_args, device_uuid_map)
device_uuid_map = register_device_list(pv_names, device_args, device_uuid_map)
device_uuid_map = register_device_list(storage_names, device_args, device_uuid_map)

aggr.device_uuid_map = device_uuid_map
print()
print('Summary of assets registered:')
print()
print(aggr.device_uuid_map)


# loop to allow persistence
while not aggr.is_finished:
    sleep(0.5)

