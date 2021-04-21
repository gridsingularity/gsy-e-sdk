# flake8: noqa

"""
Template file for markets management through the d3a API client
"""
from tabulate import tabulate
import pandas as pd
import json
import logging
from datetime import time
from time import sleep
from d3a_api_client.redis_market import RedisMarketClient
from pendulum import from_format
from d3a_interface.constants_limits import DATE_TIME_FORMAT, TIME_FORMAT
from d3a_api_client.types import aggregator_client_type
from d3a_api_client.utils import get_area_uuid_from_area_name_and_collaboration_id
from d3a_api_client.rest_market import RestMarketClient
import os

current_dir = os.path.dirname(__file__)

# List of market the Grid Operators connect to
market_names = ["Grid", "Community"]      # TODO list the market names as they are in the collaboration

# Name of your aggregator
oracle_name = "dso"

# Grid tariff selection
TimeOfUse = True            # TODO Activate the Time of Use model if set to True
Aziiz = False               # TODO Activate the Aziiz model if set to True
moving_average_peak = True  # Perform a moving average of the last #look_back ptu (for the Aziiz tariff only)
look_back = 4               # Number of past markets slots to apply the moving average (for the Aziiz tariff only)

if TimeOfUse:
    market_prices = pd.read_excel(os.path.join(current_dir, "resources/ToU.xlsx"))        # TODO upload an Excel/CSV file with prices of every market at each time slot (based on given template)
    hour = 0
    minutes = 0
    planned_fee = {}                                                        # Dictionary containing the ToU strategy previously uploaded
    for i in range(len(market_prices)):                                     # Fixed number of quarters of an hour in a day
        for j in market_names:                                              # Looping through all the markets
            planned_fee.update({(str(time(hour, minutes))[0:5], j): market_prices[j][i]})

        minutes = minutes + 15
        if minutes == 60:
            minutes = 0
            hour = hour + 1

if Aziiz:
    market_prices = pd.ExcelFile(os.path.join(current_dir, "resources/Aziiz.xlsx"))      # TODO upload an Excel/CSV file with thresholds and fees of every market (based on given template)

slot_length = 15  # leave as is


class Oracle(aggregator_client_type):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False
        self.balance_hist = pd.DataFrame(columns=market_names)

    # Function that computes the next grid fee with Time of Use strategy
    def scheduled_fee(self, market_time, planned_fee, market_name):
        slot_time = market_time.add(minutes=1*slot_length).time().format(TIME_FORMAT)             # Calculate the slot time
        if (slot_time, market_name) in planned_fee:                                               # Go through the dictionary to see if the couple time-market name is there
            next_fee = planned_fee[(slot_time, market_name)]                                      # If yes, I attribute the associated value to the next fee
        else:
            next_fee = None
        return next_fee

    # This function is triggered at the end of each market slot. It is in this function that information can be called and grid fee algorithm can be designed
    def on_market_cycle(self, market_info):  # Market info is a big dictionary containing all the information of the areas all together
        # Initialization of variables
        self.last_market_fee={}
        self.current_market_fee={}
        self.min_trade_rate={}
        self.avg_trade_rate={}
        self.max_trade_rate={}
        self.median_trade_rate={}
        self.total_traded_energy_kWh={}
        self.self_sufficiency={}
        self.self_consumption={}
        self.import_kWh={}
        self.export_kWh={}
        self.balance={}
        self.next_market_fee={}
        self.market_time = from_format(market_info["market_slot"], DATE_TIME_FORMAT)

        # In this loop dso_market_stats is called for every connected market and stored in self.dso_stats
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if area_dict["area_name"] in market_names:
                self.add_to_batch_commands.last_market_dso_stats(area_uuid)

        self.dso_stats_response = self.execute_batch_commands()

        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if area_dict["area_name"] in market_names:
                self.last_market_fee[area_dict["area_name"]] = area_dict["last_market_fee"]
                self.current_market_fee[area_dict["area_name"]] = area_dict["current_market_fee"]

        for uuid, market_event in self.dso_stats_response["responses"].items():
            # Store data from market info into variables - these variables are updated in a way that always just store the current/most updated values
            self.min_trade_rate[market_event[0]['name']] = market_event[0]["market_stats"]["min_trade_rate"]
            self.avg_trade_rate[market_event[0]['name']] = market_event[0]["market_stats"]["avg_trade_rate"]
            self.max_trade_rate[market_event[0]['name']] = market_event[0]["market_stats"]["max_trade_rate"]
            self.median_trade_rate[market_event[0]['name']] = market_event[0]["market_stats"]["median_trade_rate"]
            self.total_traded_energy_kWh[market_event[0]['name']] = market_event[0]["market_stats"]["total_traded_energy_kWh"]
            self.self_sufficiency[market_event[0]['name']] = market_event[0]["market_stats"]["self_sufficiency"]
            self.self_consumption[market_event[0]['name']] = market_event[0]["market_stats"]["self_consumption"]
            self.import_kWh[market_event[0]['name']] = market_event[0]["market_stats"]['area_throughput']['import']
            self.export_kWh[market_event[0]['name']] = market_event[0]["market_stats"]['area_throughput']['export']
            try:
                self.balance[market_event[0]['name']] = self.import_kWh[market_event[0]['name']]-self.export_kWh[market_event[0]['name']]
            except:
                self.balance[market_event[0]['name']] = 0

        self.balance_hist = self.balance_hist.append(self.balance, ignore_index=True)   # DataFrame that gets updated and grows at each market slot and contains the balance (import - export) for the areas

        # Print information in the terminal
        print('LAST MARKET STATISTICS')

        last_market_table1 = []
        for i in range(len(market_names)):
            keys_list = list(self.total_traded_energy_kWh.keys())
            values_list = list(self.total_traded_energy_kWh.values())
            min_values_list = list(self.min_trade_rate.values())
            avg_values_list = list(self.avg_trade_rate.values())
            med_values_list = list(self.median_trade_rate.values())
            max_values_list = list(self.max_trade_rate.values())

            # Print None values as 0
            values_list = [0 if x is None else x for x in values_list]
            min_values_list = [0 if x is None else x for x in min_values_list]
            avg_values_list = [0 if x is None else x for x in avg_values_list]
            med_values_list = [0 if x is None else x for x in med_values_list]
            max_values_list = [0 if x is None else x for x in max_values_list]

            last_market_table1.append([keys_list[i], values_list[i], min_values_list[i], avg_values_list[i], med_values_list[i], max_values_list[i]])

        last_market_headers1 = ["Markets", "Total energy traded [kWh]",  "Min trade rate [€cts/Kwh]", "Avg trade rate [€cts/Kwh]", "Med trade rate [€cts/Kwh]", "Max trade rate [€cts/Kwh]"]
        print(tabulate(last_market_table1, last_market_headers1, tablefmt="fancy_grid"))

        last_market_table2 = []
        for i in range(len(market_names)):
            keys_list = list(self.total_traded_energy_kWh.keys())
            ss_values_list = list(self.self_sufficiency.values())
            sc_values_list = list(self.self_consumption.values())
            energy_imp_values_list = list(self.import_kWh.values())
            energy_exp_values_list = list(self.export_kWh.values())
            last_fee_values_list = list(self.last_market_fee.values())
            # Print None values as 0
            ss_values_list = [0 if x is None else x for x in ss_values_list]
            sc_values_list = [0 if x is None else x for x in sc_values_list]
            energy_imp_values_list = [0 if x is None else x for x in energy_imp_values_list]
            energy_exp_values_list = [0 if x is None else x for x in energy_exp_values_list]
            last_fee_values_list = [0 if x is None else x for x in last_fee_values_list]

            last_market_table2.append([keys_list[i], ss_values_list[i], sc_values_list[i], energy_imp_values_list[i], energy_exp_values_list[i], last_fee_values_list[i]])

        last_market_headers2 = ["Markets", "Self sufficiency [%]", "Self consumption [%]", "Energy import [kWh]", "Energy export [kWh]", "Last fee [€cts/Kwh]"]
        print(tabulate(last_market_table2, last_market_headers2, tablefmt="fancy_grid"))


        # This loop is used to set the batch command for the new grid fee
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():

            if area_dict["area_name"] in market_names:
                ###################################################################
                # TIME OF USE STRATEGY
                ###################################################################
                if TimeOfUse:
                    self.next_market_fee[area_dict["area_name"]] = self.scheduled_fee(self.market_time, planned_fee, area_dict["area_name"])

                ###################################################################
                # AZIIZ STRATEGY
                ###################################################################
                if Aziiz:

                    if moving_average_peak:
                        self.max_ext_energy_kWh = abs(self.balance_hist[area_dict["area_name"]].iloc[-look_back:].mean())                      # Calculate the absolute moving average of the net balance by looking back at 4 markets
                    else:
                        self.max_ext_energy_kWh = max(self.import_kWh[area_dict["area_name"]], self.export_kWh[area_dict["area_name"]])            # Calculate the max between the import and export of the defined market

                    individual_market_prices = pd.read_excel(market_prices, area_dict["area_name"])                 # Going into the correct sheet

                    for k in range(len(individual_market_prices)):
                        if self.max_ext_energy_kWh <= individual_market_prices["Threshold"][k]:                 # if the peak is lower than this value, the following grid fee will be applied. If the max is higher, it will go to the lower elif condition
                            self.next_market_fee[area_dict["area_name"]] = individual_market_prices["Grid fee"][k]  # new grid fee
                            break
                        else:
                            self.next_market_fee[area_dict["area_name"]] = 2000                                     # TODO set the last value if all the previous ones are not fulfilled

                self.add_to_batch_commands.grid_fees(area_uuid=area_uuid, fee_cents_kwh=self.next_market_fee[area_dict["area_name"]])

        next_fee_response = self.execute_batch_commands()  # send batch command

        print()
        print('CURRENT AND NEXT MARKET STATISTICS')

        current_market_table = []
        for i in range(len(market_names)):
            keys_list = list(self.current_market_fee.keys())
            current_fee_values_list = list(self.current_market_fee.values())
            next_fee_values_list = list(self.next_market_fee.values())

            current_fee_values_list = [0 if x is None else x for x in current_fee_values_list]
            next_fee_values_list = [0 if x is None else x for x in next_fee_values_list]

            current_market_table.append([keys_list[i], current_fee_values_list[i], next_fee_values_list[i]])

        current_market_headers = ["Markets", "Current fee [€cts/kWh]", "Next fee [€cts/kWh]"]
        print(tabulate(current_market_table, current_market_headers, tablefmt="fancy_grid"))

    def on_finish(self, finish_info):  # This function is triggered when the simulation has ended
        self.is_finished = True

    def on_batch_response(self, market_stats):
        pass


# Code to create the market aggregator and connect to the relevant simulation. KEEP IT AS IS
if os.environ["API_CLIENT_RUN_ON_REDIS"] == "true":
    MarketClient = RedisMarketClient
    market_args = {"autoregister": True}
    aggr = Oracle(aggregator_name=oracle_name)

else:
    MarketClient = RestMarketClient
    if os.environ['SIMULATION_CONFIG_FILE_PATH'] is not None:
        with open(os.environ['SIMULATION_CONFIG_FILE_PATH']) as json_file:
            simulation_info = json.load(json_file)
            simulation_id = simulation_info['uuid']
            domain_name = simulation_info['domain_name']
            websockets_domain_name = simulation_info['web_socket_domain_name']
    else:
        simulation_id = os.environ["API_CLIENT_SIMULATION_ID"]
        domain_name = os.environ["API_CLIENT_DOMAIN_NAME"]
        websockets_domain_name = os.environ["API_CLIENT_WEBSOCKET_DOMAIN_NAME"]

    market_args = {"simulation_id": simulation_id,
                   "domain_name": domain_name, "websockets_domain_name":websockets_domain_name}

    aggr = Oracle(aggregator_name=oracle_name, **market_args)

print()
print('Connecting to markets ...')

for i in market_names:
    if os.environ["API_CLIENT_RUN_ON_REDIS"] == "true":
        market_registered = RedisMarketClient(i)
    else:
        # Creating a connection to a market
        market_registered_uuid = get_area_uuid_from_area_name_and_collaboration_id(
            market_args["simulation_id"], i, market_args["domain_name"])
        market_args["area_id"] = market_registered_uuid
        market_registered = RestMarketClient(**market_args)
    market_registered
    selected = market_registered.select_aggregator(aggr.aggregator_uuid)
    logging.info(f"SELECTED: {selected}")
    print("----> Connected to ", i)
    sleep(0.3)

print(aggr.device_uuid_list)

# loop to allow persistence
while not aggr.is_finished:
    sleep(0.5)
