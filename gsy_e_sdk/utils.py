import ast
import json
import logging
import os
from functools import wraps
from typing import Optional, Dict
from tabulate import tabulate

import requests
from gsy_framework.api_simulation_config.validators import validate_api_simulation_config
from gsy_framework.utils import get_area_name_uuid_mapping
from sgqlc.endpoint.http import HTTPEndpoint

from gsy_e_sdk import __version__
from gsy_e_sdk.constants import (
    DEFAULT_DOMAIN_NAME, DEFAULT_WEBSOCKET_DOMAIN,
    CUSTOMER_WEBSOCKET_DOMAIN_NAME, API_CLIENT_SIMULATION_ID)

CONSUMER_WEBSOCKET_DOMAIN_NAME_FROM_ENV = os.environ.get("CUSTOMER_WEBSOCKET_DOMAIN_NAME",
                                                         CUSTOMER_WEBSOCKET_DOMAIN_NAME)


def domain_name_from_env() -> str:
    """Return the domain name's environment variable if set."""
    return os.environ.get("API_CLIENT_DOMAIN_NAME", DEFAULT_DOMAIN_NAME)


def websocket_domain_name_from_env() -> str:
    """Return the websocket domain name's environment variable if set."""
    return os.environ.get("API_CLIENT_WEBSOCKET_DOMAIN_NAME", DEFAULT_WEBSOCKET_DOMAIN)


def simulation_id_from_env() -> str:
    """Return the simulation ID environment variable if set."""
    return os.environ.get("API_CLIENT_SIMULATION_ID", API_CLIENT_SIMULATION_ID)


class AreaNotFoundException(Exception):
    """Exception denoting that an area was not found in a simulation."""


def execute_graphql_request(domain_name: str, query: str,
                            headers=None, url=None, authenticate=True):
    """
    Fire a graphql request to the desired url and returns the response
    """
    jwt_key = None
    if authenticate:
        jwt_key = retrieve_jwt_key_from_server(domain_name)
        if jwt_key is None:
            logging.error("authentication failed")
            return None
    url = f"{domain_name}/graphql/" if url is None else url
    headers = {"Authorization": f"JWT {jwt_key}",
               "Content-Type": "application/json"} if headers is None else headers
    endpoint = HTTPEndpoint(url, headers)
    data = endpoint(query=query)
    return data


def retrieve_jwt_key_from_server(domain_name: str) -> Optional[str]:
    """
    Get the jwt token from the server based on credentials set in the
    environment variables.
    """
    resp = requests.post(
        f"{domain_name}/api-token-auth/",
        data=json.dumps({"username": os.environ["API_CLIENT_USERNAME"],
                         "password": os.environ["API_CLIENT_PASSWORD"]}),
        headers={"Content-Type": "application/json"})
    if resp.status_code != 200:
        logging.error("Request for token authentication failed with status "
                      "code %s. Response body: %s", resp.status_code, resp.text)
        return None

    validate_client_up_to_date(resp)
    return json.loads(resp.text)["token"]


def get_aggregator_prefix(domain_name: str,
                          simulation_id: Optional[str] = None) -> str:
    """Build a prefix for the aggregator API endpoint."""
    return f"{domain_name}/external-connection/aggregator-api/{simulation_id}/"


def get_configuration_prefix(domain_name: str,
                             simulation_id: Optional[str] = None) -> str:
    """Build a prefix for the configurations API endpoint."""
    return f"{domain_name}/external-connection/configurations/{simulation_id}/"


def get_area_uuid_from_area_name(serialized_scenario: dict,
                                 area_name: str) -> Optional[str]:
    """
    Iterate over a scenario representation and look up the uuid of the
    area that name matches area_name.
    """
    if "name" in serialized_scenario and serialized_scenario["name"] == area_name:
        return serialized_scenario["uuid"]
    if "children" in serialized_scenario:
        for child in serialized_scenario["children"]:
            area_uuid = get_area_uuid_from_area_name(child, area_name)
            if area_uuid is not None:
                return area_uuid
    return None


def get_area_uuid_from_area_name_and_collaboration_id(
        collab_id, area_name, domain_name) -> str:
    """
    Fire a request to get the scenario representation of the collaboration and
    search for the uuid of the area that name matches area_name.
    """
    query = '''query { readConfiguration(uuid: "''' + collab_id + '''")
                { scenarioData { latest { serialized } } } }'''
    data = execute_graphql_request(domain_name=domain_name, query=query)
    area_uuid = get_area_uuid_from_area_name(
        json.loads(data["data"]["readConfiguration"]["scenarioData"]["latest"]["serialized"]),
        area_name
    )
    if not area_uuid:
        raise AreaNotFoundException(f"Area with name {area_name} is not part of the "
                                    f"collaboration with UUID {collab_id}")
    return area_uuid


def get_area_uuid_and_name_mapping_from_simulation_id(
        collab_id: str, domain_name: str = None) -> dict:
    """
    Fire a request to get the scenario representation of the collaboration and
    map for the uuid of the areas to their names.
    """
    query = '''query { readConfiguration(uuid: "''' + collab_id + '''")
                { scenarioData { latest { serialized } } } }'''

    data = execute_graphql_request(domain_name=domain_name or domain_name_from_env(), query=query)
    if data.get("errors"):
        return ast.literal_eval(data["errors"][0]["message"])
    area_name_uuid_map = get_area_name_uuid_mapping(
        json.loads(data["data"]["readConfiguration"]["scenarioData"]["latest"]["serialized"])
    )
    return area_name_uuid_map


def get_aggregators_list(domain_name: Optional[str] = None) -> list:
    """
    Return a list of aggregators for the logged in user.
    """
    if not domain_name:
        domain_name = os.environ.get("API_CLIENT_DOMAIN_NAME")
    query = "query { aggregatorsList { configUuid name  devicesList { deviceUuid } } }"

    data = execute_graphql_request(domain_name=domain_name, query=query)
    return (ast.literal_eval(data["errors"][0]["message"]) if
            data.get("errors") else data["data"]["aggregatorsList"])


def logging_decorator(command_name: str):
    """
    Decorator that logs the commands performed before executing them and
    return the response afterwords.
    """
    def decorator(function: callable):
        @wraps(function)
        def wrapped(self, *args, **kwargs):
            logging.debug("Sending command %s to device.", command_name)
            return_value = function(self, *args, **kwargs)
            logging.debug("Command %s responded with: %s.", command_name, return_value)
            return return_value
        return wrapped
    return decorator


def list_running_canary_networks_and_devices_with_live_data(domain_name) -> dict:
    """Return all canary networks with their forecastStreamAreaMapping setting."""
    query = '''
    query {
      listCanaryNetworks {
        configurations {
          uuid
          resultsStatus
          scenarioData {
            forecastStreamAreaMapping
          }
        }
      }
    }
    '''
    data = execute_graphql_request(domain_name=domain_name, query=query)

    logging.debug("Received Canary Network data: %s", data)

    return {
        cn["uuid"]: cn["scenarioData"]["forecastStreamAreaMapping"]
        for cn in data["data"]["listCanaryNetworks"]["configurations"]
        if cn["resultsStatus"] == "running"
    }


def log_bid_offer_confirmation(message: dict) -> None:
    """Log the details of orders placed in the markets."""
    try:
        if message.get("status") == "ready" and message.get("command") in ["bid", "offer"]:
            event = "bid" if "bid" in message.get("command") else "offer"
            data_dict = json.loads(message.get(event))
            market_type = message.get("market_type")
            energy = data_dict.get("energy")
            price = data_dict.get("price")
            rate = price / energy
            trader = data_dict.get("seller" if event == "offer" else "buyer")
            action = "OFFERED" if event == "offer" else "BID"
            logging.info("[%s] %s %s %s kWh at %s cts/kWh",
                         market_type, trader, action, round(energy, 3), rate)
    # pylint: disable = broad-except
    except Exception:
        logging.exception("Logging bid/offer info failed.")


def log_deleted_bid_offer_confirmation(
        message: dict, command_type: Optional[str] = None,
        bid_offer_id: Optional[str] = None, asset_name: Optional[str] = None) -> None:
    """Log the details of orders deleted from the markets."""
    try:
        if message.get("status") == "ready" and message.get("command") in ["bid_delete",
                                                                           "offer_delete"]:
            if command_type is None:
                # For the aggregator response, command type is not explicitly provided
                command_type = "bid" if "bid" in message.get("command") else "offer"
            if bid_offer_id is None:
                logging.info(
                    "<-- All %ss of %s are successfully deleted-->", command_type, asset_name)
            else:
                logging.info(
                    "<-- %s %s is successfully deleted-->", command_type, bid_offer_id)
    # pylint: disable = broad-except
    except Exception:
        logging.exception("Logging bid/offer deletion confirmation failed.")


def log_trade_info(message):
    """Log the details of trades matched in the markets."""
    rate = round(message.get("trade_price") / message.get("traded_energy"), 2)
    if message.get("buyer") == "anonymous":
        logging.info(
            "<-- %s SOLD %s kWh at %s cents/kWh -->",
            message.get("seller"), round(message.get("traded_energy"), 3), rate)
    else:
        logging.info(
            "<-- %s BOUGHT %s kWh at %s cents/kWh -->",
            message.get("buyer"), round(message.get("traded_energy"), 3), rate)


def flatten_info_dict(indict: dict) -> dict:
    """
    wrapper for _flatten_info_dict
    """
    if indict == {}:
        return {}
    outdict = {}
    _flatten_info_dict(indict, outdict)
    return outdict


def _flatten_info_dict(indict: dict, outdict: dict):
    """
    Flattens market_info/tick_info information trees
    outdict will hold references to all area subdicts of indict
    """
    for area_name, area_dict in indict.items():
        outdict[area_name] = area_dict
        if "children" in area_dict:
            _flatten_info_dict(indict[area_name]["children"], outdict)


def get_uuid_from_area_name_in_tree_dict(
        area_name_uuid_mapping: dict, name: str) -> str:
    """Look up the area uuid in the area_name_uuid_mapping based on its name."""
    if name not in area_name_uuid_mapping:
        raise ValueError(f"Could not find {name} in tree")
    if len(area_name_uuid_mapping[name]) == 1:
        return area_name_uuid_mapping[name][0]
    raise ValueError(f"There are multiple areas named {name} in the tree")


def buffer_grid_tree_info(function: callable):
    """Update the grid_tree details of the calling class."""
    @wraps(function)
    def wrapper(self, message):
        self.latest_grid_tree = message["grid_tree"]
        self.latest_grid_tree_flat = flatten_info_dict(self.latest_grid_tree)
        function(self, message)
    return wrapper


def create_area_name_uuid_mapping_from_tree_info(latest_grid_tree_flat: dict) -> dict:
    """Build up a {area_name: [area_uuids]} mapping from the latest_grid_tree_flat."""
    area_name_uuid_mapping = {}
    for area_uuid, area_dict in latest_grid_tree_flat.items():
        if "area_name" in area_dict:
            if area_uuid in area_name_uuid_mapping:
                area_name_uuid_mapping[area_dict["area_name"]].append(area_uuid)
            else:
                area_name_uuid_mapping[area_dict["area_name"]] = [area_uuid]
    return area_name_uuid_mapping


def read_simulation_config_file(config_file_path: str) -> Optional[Dict]:
    """Return simulation config as dict if config_file_path is provided."""
    if config_file_path:
        with open(config_file_path, encoding="utf-8") as json_file:
            simulation_config = json.load(json_file)
        validate_api_simulation_config(simulation_config)
        return simulation_config
    return None


def get_sim_id_and_domain_names():
    """Return the simulation id, domain name and websocket domain name from the environment."""
    return simulation_id_from_env(), domain_name_from_env(), websocket_domain_name_from_env()


def validate_client_up_to_date(response):
    """Check whether the client is connected to the supporting version of the server."""
    remote_version = response.headers.get("API-VERSION")
    if not remote_version:
        return

    if __version__ < remote_version:
        logging.warning(
            "Your version of the client %s is outdated, kindly upgrade to "
            "version %s to make use of our latest features", __version__, remote_version)


def get_name_from_area_name_uuid_mapping(area_name_uuid_mapping, asset_uuid):
    """Get the area name from the name: [uuids] mapping."""
    for area_name, area_uuids in area_name_uuid_mapping.items():
        for area_uuid in area_uuids:
            if area_uuid == asset_uuid:
                return area_name
    return None


def log_grid_fees_information(market_names, current_market_fee, next_market_fee):
    """Log information about the current and next grid fees for each market"""
    current_market_table = []
    for i in range(len(market_names)):
        current_market_table.append(
            [
                list(current_market_fee.keys())[i],
                list(current_market_fee.values())[i],
                list(next_market_fee.values())[i],
            ]
        )
    current_market_headers = [
        "Market name",
        "Current fee [€cts/kWh]",
        "Next fee [€cts/kWh]",
    ]
    logging.info(
        "\n%s",
        tabulate(current_market_table, current_market_headers, tablefmt="fancy_grid"),
    )


def get_assets_name(registry: Dict) -> Dict:
    """
    Parse the grid tree and return all registered assets / markets
    Wrapper for _get_assets_name
    """
    if registry == {}:
        return {}
    reg_assets = {"Area": [], "Load": [], "PV": [], "Storage": []}
    _get_assets_name(registry, reg_assets)
    return reg_assets


def _get_assets_name(node: Dict, reg_assets: Dict):
    """
    Parse the Collaboration / Canary Network registry
    Return a list of the Asset / Market nodes the user is registered to
    """
    if node.get("registered") is True:
        area_type = node["type"]
        reg_assets[area_type].append(node["name"])
    for child in node.get("children", []):
        _get_assets_name(child, reg_assets)
