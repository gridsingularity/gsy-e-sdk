# flake8: noqa
# pylint: disable=duplicate-code

"""
Template file for markets management through the gsy-e-sdk api client
"""
import os
import json
from time import sleep
from gsy_e_sdk.types import aggregator_client_type
from gsy_e_sdk.rest_market import RestMarketClient
from gsy_e_sdk.utils import log_grid_fees_information
from gsy_e_sdk.utils import get_area_uuid_from_area_name_and_collaboration_id


current_dir = os.path.dirname(__file__)

market_names = [
    "Grid",
    "Community",
]
ORACLE_NAME = "dso"
SLOT_LENGTH = 15
MOVING_AVERAGE_PEAK = True
LOOK_BACK = 4
AUTOMATIC = True


class Oracle(aggregator_client_type):
    """Class that defines the behaviour of an "oracle" aggregator."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False
        self.import_kwh = {}
        self.export_kwh = {}
        self.balance_hist = []
        self.balance = {}
        self.dso_stats_response = {}

    def calculate_import_export_balance(self):
        """Calculate the balance (import - export) for each market."""
        self.dso_stats_response = self.execute_batch_commands()
        for market_event in self.dso_stats_response["responses"].items():
            self.import_kwh[market_event[1][0]["name"]] = market_event[1][0][
                "market_stats"
            ]["area_throughput"]["import"]
            self.export_kwh[market_event[1][0]["name"]] = market_event[1][0][
                "market_stats"
            ]["area_throughput"]["export"]
            self.balance[market_event[1][0]["name"]] = (
                self.import_kwh[market_event[1][0]["name"]]
                - self.export_kwh[market_event[1][0]["name"]]
            )
        self.balance_hist.append(self.balance)

    def set_new_market_fee(self):
        """Return the market fees for each market for the next time slot."""
        next_market_fee = {}
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if area_dict["area_name"] in market_names:
                if MOVING_AVERAGE_PEAK:
                    fees = []
                    # pylint: disable=unused-variable
                    for j, k in enumerate(self.balance_hist):
                        fees.append(k[area_dict["area_name"]])
                    max_ext_energy_kwh = abs(
                        sum(fees[-LOOK_BACK:]) / len(fees[-LOOK_BACK:])
                    )
                else:
                    max_ext_energy_kwh = max(
                        self.import_kwh[area_dict["area_name"]],
                        self.export_kwh[area_dict["area_name"]],
                    )

                individual_market_fees = fee_strategy[area_dict["area_name"]]
                # pylint: disable=unused-variable
                for j, k in enumerate(individual_market_fees):
                    if (
                            max_ext_energy_kwh
                            <= k["Import / Export threshold"]
                    ):
                        next_market_fee[
                            area_dict["area_name"]
                        ] = k["Grid fee"]
                        break
                    next_market_fee[area_dict["area_name"]] = 2000
                self.add_to_batch_commands.grid_fees(
                    area_uuid=area_uuid,
                    fee_cents_kwh=next_market_fee[area_dict["area_name"]],
                )
        return next_market_fee

    def on_market_cycle(self, market_info):
        current_market_fee = {}
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if area_dict["area_name"] in market_names:
                self.add_to_batch_commands.last_market_dso_stats(area_uuid)
                current_market_fee[area_dict["area_name"]] = area_dict[
                    "current_market_fee"
                ]
        self.calculate_import_export_balance()
        next_market_fee = self.set_new_market_fee()
        self.execute_batch_commands()
        log_grid_fees_information(market_names, current_market_fee, next_market_fee)

    def on_event_or_response(self, message):
        pass

    def on_finish(self, finish_info):
        self.is_finished = True


def read_fee_strategy():
    "Return a dictionary containing the Aziiz strategy loaded from the JSON input file."
    with open(
        os.path.join(current_dir, "resources/Aziiz.json"),
        "r",
        encoding="utf-8",
    ) as file:
        aziiz_fee = json.loads(file.read())
    return aziiz_fee


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


MarketClient = RestMarketClient
market_args = {
    "simulation_id": os.environ["API_CLIENT_SIMULATION_ID"],
    "domain_name": os.environ["API_CLIENT_DOMAIN_NAME"],
    "websockets_domain_name": os.environ["API_CLIENT_WEBSOCKET_DOMAIN_NAME"],
}

aggr = Oracle(aggregator_name=ORACLE_NAME, **market_args)
if AUTOMATIC:
    registry = aggr.get_configuration_registry()
    market_names = get_assets_name(registry)["Area"]

fee_strategy = read_fee_strategy()

print()
print("Connecting to markets ...")

for i in market_names:
    market_uuid = get_area_uuid_from_area_name_and_collaboration_id(
        market_args["simulation_id"], i, market_args["domain_name"]
    )
    market_args["area_id"] = market_uuid
    market_registered = RestMarketClient(**market_args)
    market_registered.select_aggregator(aggr.aggregator_uuid)
    print("----> Connected to ", i)
    sleep(0.3)

print(aggr.device_uuid_list)

# loop to allow persistence
while not aggr.is_finished:
    sleep(0.5)
