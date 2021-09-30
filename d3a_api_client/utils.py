import ast
import json
import logging
import os
from functools import wraps

import requests
from d3a_interface.api_simulation_config.validators import validate_api_simulation_config
from d3a_interface.utils import get_area_name_uuid_mapping, key_in_dict_and_not_none
from sgqlc.endpoint.http import HTTPEndpoint
from tabulate import tabulate

from d3a_api_client import __version__
from d3a_api_client.constants import DEFAULT_DOMAIN_NAME, DEFAULT_WEBSOCKET_DOMAIN, \
    CUSTOMER_WEBSOCKET_DOMAIN_NAME, API_CLIENT_SIMULATION_ID

CONSUMER_WEBSOCKET_DOMAIN_NAME_FROM_ENV = os.environ.get("CUSTOMER_WEBSOCKET_DOMAIN_NAME",
                                                         CUSTOMER_WEBSOCKET_DOMAIN_NAME)


def domain_name_from_env():
    return os.environ.get("API_CLIENT_DOMAIN_NAME", DEFAULT_DOMAIN_NAME)


def websocket_domain_name_from_env():
    return os.environ.get("API_CLIENT_WEBSOCKET_DOMAIN_NAME", DEFAULT_WEBSOCKET_DOMAIN)


def simulation_id_from_env():
    return os.environ.get("API_CLIENT_SIMULATION_ID", API_CLIENT_SIMULATION_ID)


class AreaNotFoundException(Exception):
    pass


class RedisAPIException(Exception):
    pass


def execute_graphql_request(domain_name, query, headers=None, url=None, authenticate=True):
    """
    Fires a graphql request to the desired url and returns the response
    """
    jwt_key = None
    if authenticate:
        jwt_key = retrieve_jwt_key_from_server(domain_name)
        if jwt_key is None:
            logging.error(f"authentication failed")
            return
    url = f"{domain_name}/graphql/" if url is None else url
    headers = {'Authorization': f'JWT {jwt_key}', 'Content-Type': 'application/json'} \
        if headers is None else headers
    endpoint = HTTPEndpoint(url, headers)
    data = endpoint(query=query)
    return data


def retrieve_jwt_key_from_server(domain_name):
    resp = requests.post(
        f"{domain_name}/api-token-auth/",
        data=json.dumps({"username": os.environ["API_CLIENT_USERNAME"],
                         "password": os.environ["API_CLIENT_PASSWORD"]}),
        headers={"Content-Type": "application/json"})
    if resp.status_code != 200:
        logging.error(f"Request for token authentication failed with status code {resp.status_code}. "
                      f"Response body: {resp.text}")
        return

    validate_client_up_to_date(resp)
    return json.loads(resp.text)["token"]


def get_aggregator_prefix(domain_name, simulation_id=None):
    return f"{domain_name}/external-connection/aggregator-api/{simulation_id}/"


def get_configuration_prefix(domain_name, simulation_id=None):
    return f"{domain_name}/external-connection/configurations/{simulation_id}/"


def get_area_uuid_from_area_name(serialized_scenario, area_name):
    if "name" in serialized_scenario and serialized_scenario["name"] == area_name:
        return serialized_scenario["uuid"]
    if "children" in serialized_scenario:
        for child in serialized_scenario["children"]:
            area_uuid = get_area_uuid_from_area_name(child, area_name)
            if area_uuid is not None:
                return area_uuid
    return None


def get_area_uuid_from_area_name_and_collaboration_id(collab_id, area_name, domain_name):
    query = 'query { readConfiguration(uuid: "{' + collab_id + \
            '}") { scenarioData { latest { serialized } } } }'
    data = execute_graphql_request(domain_name=domain_name, query=query)
    area_uuid = get_area_uuid_from_area_name(
        json.loads(data["data"]["readConfiguration"]["scenarioData"]["latest"]["serialized"]), area_name
    )
    if not area_uuid:
        raise AreaNotFoundException(f"Area with name {area_name} is not part of the "
                                    f"collaboration with UUID {collab_id}")
    return area_uuid


def get_area_uuid_and_name_mapping_from_simulation_id(collab_id):
    query = 'query { readConfiguration(uuid: "{' + collab_id + \
            '}") { scenarioData { latest { serialized } } } }'

    data = execute_graphql_request(domain_name=domain_name_from_env(), query=query)
    if key_in_dict_and_not_none(data, 'errors'):
        return ast.literal_eval(data['errors'][0]['message'])
    else:
        area_name_uuid_map = get_area_name_uuid_mapping(
            json.loads(data["data"]["readConfiguration"]["scenarioData"]["latest"]["serialized"])
        )
        return area_name_uuid_map


def get_aggregators_list(domain_name=None):
    """
    Returns a list of aggregators for the logged in user
    """
    if not domain_name:
        domain_name = os.environ.get("API_CLIENT_DOMAIN_NAME")
    query = 'query { aggregatorsList { configUuid name  devicesList { deviceUuid } } }'

    data = execute_graphql_request(domain_name=domain_name, query=query)
    return ast.literal_eval(data["errors"][0]["message"]) if \
        key_in_dict_and_not_none(data, "errors") else data["data"]["aggregatorsList"]


def logging_decorator(command_name):
    def decorator(f):
        @wraps(f)
        def wrapped(self, *args, **kwargs):
            logging.debug(f'Sending command {command_name} to device.')
            return_value = f(self, *args, **kwargs)
            logging.debug(f'Command {command_name} responded with: {return_value}.')
            return return_value
        return wrapped
    return decorator


def list_running_canary_networks_and_devices_with_live_data(domain_name):
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

    logging.debug(f"Received Canary Network data: {data}")

    return {
        cn["uuid"]: cn["scenarioData"]["forecastStreamAreaMapping"]
        for cn in data["data"]["listCanaryNetworks"]["configurations"]
        if cn["resultsStatus"] == "running"
    }


def log_bid_offer_confirmation(message):
    try:
        if message.get("status") == "ready" and message.get("command") in ["bid", "offer"]:
            event = "bid" if "bid" in message.get("command") else "offer"
            data_dict = json.loads(message.get(event))
            energy = data_dict.get("energy")
            price = data_dict.get("price")
            rate = price / energy
            trader = data_dict.get("seller" if event == "offer" else "buyer")
            logging.info(f"{trader} {'OFFERED' if event == 'offer' else 'BID'} "
                         f"{round(energy, 3)} kWh at {rate} cts/kWh")
    except Exception as e:
        logging.exception("Logging bid/offer info failed.%s", str(e))


def log_deleted_bid_offer_confirmation(message, command_type=None, bid_offer_id=None,
                                       asset_name=None):
    try:
        if message.get("status") == "ready" and message.get("command") in ["bid_delete",
                                                                           "offer_delete"]:
            if command_type is None:
                # For the aggregator response, command type is not explicitly provided
                command_type = "bid" if "bid" in message.get("command") else "offer"
            if bid_offer_id is None:
                logging.info(
                    f"<-- All {command_type}s of {asset_name} are successfully deleted-->")
            else:
                logging.info(
                    f"<-- {command_type} {bid_offer_id} is successfully deleted-->")
    except Exception as e:
        logging.error(f"Logging bid/offer deletion confirmation failed.{e}")


def log_trade_info(message):
    rate = round(message.get('trade_price') / message.get('traded_energy'), 2)
    if message.get("buyer") == "anonymous":
        logging.info(
            f"<-- {message.get('seller')} SOLD {round(message.get('traded_energy'), 3)} kWh "
            f"at {rate} cents/kWh -->")
    else:
        logging.info(
            f"<-- {message.get('buyer')} BOUGHT {round(message.get('traded_energy'), 3)} kWh "
            f"at {rate} cents/kWh -->")


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
        if 'children' in area_dict:
            _flatten_info_dict(indict[area_name]['children'], outdict)


def get_uuid_from_area_name_in_tree_dict(area_name_uuid_mapping, name):
    if name not in area_name_uuid_mapping:
        raise ValueError(f"Could not find {name} in tree")
    if len(area_name_uuid_mapping[name]) == 1:
        return area_name_uuid_mapping[name][0]
    else:
        ValueError(f"There are multiple areas named {name} in the tree")


def buffer_grid_tree_info(f):
    @wraps(f)
    def wrapper(self, message):
        self.latest_grid_tree = message["grid_tree"]
        self.latest_grid_tree_flat = flatten_info_dict(self.latest_grid_tree)
        f(self, message)
    return wrapper


def create_area_name_uuid_mapping_from_tree_info(latest_grid_tree_flat: dict) -> dict:
    area_name_uuid_mapping = {}
    for area_uuid, area_dict in latest_grid_tree_flat.items():
        if "area_name" in area_dict:
            if area_uuid in area_name_uuid_mapping:
                area_name_uuid_mapping[area_dict["area_name"]].append(area_uuid)
            else:
                area_name_uuid_mapping[area_dict["area_name"]] = [area_uuid]
    return area_name_uuid_mapping


def read_simulation_config_file(config_file_path):
    if config_file_path:
        with open(config_file_path) as json_file:
            simulation_config = json.load(json_file)
        validate_api_simulation_config(simulation_config)
        return simulation_config
    else:
        raise ValueError("SIMULATION_CONFIG_FILE_PATH environmental variable must be provided ")


def get_sim_id_and_domain_names():
    return simulation_id_from_env(), domain_name_from_env(), websocket_domain_name_from_env()


def validate_client_up_to_date(response):
    remote_version = response.headers.get("API-VERSION")
    if not remote_version:
        return

    if __version__ < remote_version:
        logging.warning(
            f"Your version of the client {__version__} is outdated, kindly upgrade to "
            f"version {remote_version} to make use of our latest features")


def get_name_from_area_name_uuid_mapping(area_name_uuid_mapping, asset_uuid):
    for area_name, area_uuids in area_name_uuid_mapping.items():
        for area_uuid in area_uuids:
            if area_uuid == asset_uuid:
                return area_name

