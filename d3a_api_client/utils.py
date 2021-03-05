import os
import traceback
import ast
import requests
import json
import logging
import uuid
from functools import wraps
from tabulate import tabulate
from sgqlc.endpoint.http import HTTPEndpoint
from d3a_interface.utils import key_in_dict_and_not_none, get_area_name_uuid_mapping, \
    RepeatingTimer
from d3a_interface.constants_limits import JWT_TOKEN_EXPIRY_IN_SECS
from d3a_api_client.constants import DEFAULT_DOMAIN_NAME, DEFAULT_WEBSOCKET_DOMAIN, \
    CONSUMER_WEBSOCKET_DOMAIN_NAME


class AreaNotFoundException(Exception):
    pass


class RedisAPIException(Exception):
    pass


class RestCommunicationMixin:

    def _create_jwt_refresh_timer(self, sim_api_domain_name):
        self.jwt_refresh_timer = RepeatingTimer(
            JWT_TOKEN_EXPIRY_IN_SECS - 30, self._refresh_jwt_token, [sim_api_domain_name]
        )
        self.jwt_refresh_timer.daemon = True
        self.jwt_refresh_timer.start()

    def _refresh_jwt_token(self, domain_name):
        self.jwt_token = retrieve_jwt_key_from_server(domain_name)

    @property
    def _url_prefix(self):
        return f'{self.domain_name}/external-connection/api/{self.simulation_id}/{self.device_id}'

    def _post_request(self, endpoint_suffix, data):
        endpoint = f"{self._url_prefix}/{endpoint_suffix}/"
        data["transaction_id"] = str(uuid.uuid4())
        return data["transaction_id"], post_request(endpoint, data, self.jwt_token)

    def _get_request(self, endpoint_suffix, data):
        endpoint = f"{self._url_prefix}/{endpoint_suffix}/"
        data["transaction_id"] = str(uuid.uuid4())
        return data["transaction_id"], get_request(endpoint, data, self.jwt_token)


def execute_graphql_request(domain_name, query, headers=None, url=None, authenticate=True):
    """
    Fires a graphql request to the desired url and returns the response
    """
    jwt_key = None
    if authenticate:
        jwt_key = retrieve_jwt_key_from_server(domain_name)
        if jwt_key is None:
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
    return json.loads(resp.text)["token"]


def post_request(endpoint, data, jwt_token):
    resp = requests.post(
        endpoint,
        data=json.dumps(data),
        headers={"Content-Type": "application/json",
                 "Authorization": f"JWT {jwt_token}"})
    return json.loads(resp.text) if request_response_returns_http_2xx(endpoint, resp) else None


def blocking_post_request(endpoint, data, jwt_token):
    data["transaction_id"] = str(uuid.uuid4())
    resp = requests.post(
        endpoint,
        data=json.dumps(data),
        headers={"Content-Type": "application/json",
                 "Authorization": f"JWT {jwt_token}"})
    return json.loads(resp.text) if request_response_returns_http_2xx(endpoint, resp) else None


def get_request(endpoint, data, jwt_token):
    resp = requests.get(
        endpoint,
        data=json.dumps(data),
        headers={"Content-Type": "application/json",
                 "Authorization": f"JWT {jwt_token}"})
    return request_response_returns_http_2xx(endpoint, resp)


def request_response_returns_http_2xx(endpoint, resp):
    if 200 <= resp.status_code <= 299:
        return True
    else:
        logging.error(f"Request to {endpoint} failed with status code {resp.status_code}. "
                      f"Response body: {resp.text}")
        return False


def get_aggregator_prefix(domain_name, simulation_id):
    return f"{domain_name}/external-connection/aggregator-api/{simulation_id}/"


def blocking_get_request(endpoint, data, jwt_token):
    data["transaction_id"] = str(uuid.uuid4())
    resp = requests.get(
        endpoint,
        data=json.dumps(data),
        headers={"Content-Type": "application/json",
                 "Authorization": f"JWT {jwt_token}"})
    return json.loads(resp.json()) if request_response_returns_http_2xx(endpoint, resp) else None


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


def get_area_uuid_and_name_mapping_from_simulation_id(collab_id, domain_name):
    query = 'query { readConfiguration(uuid: "{' + collab_id + \
            '}") { scenarioData { latest { serialized } } } }'

    data = execute_graphql_request(domain_name=domain_name, query=query)
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


def execute_function_util(function: callable, function_name):
    try:
        function()
    except Exception as e:
        logging.error(
            f"{function_name} raised exception: {str(e)}. \n Traceback: {str(traceback.format_exc())}")


def log_market_progression(message):
    try:
        event = message.get("event", None)
        if event not in ["tick", "market"]:
            return
        headers = ["event", ]
        table_data = [event, ]
        data_dict = message.get("content")[0] if "content" in message.keys() else message
        if "slot_completion" in data_dict:
            headers.append("slot_completion")
            table_data.append(data_dict.get("slot_completion"))
        if "start_time" in data_dict:
            headers.extend(["start_time", "duration_min", ])
            table_data.extend([data_dict.get("start_time"), data_dict.get("duration_min")])

        logging.info(f"\n\n{tabulate([table_data, ], headers=headers, tablefmt='fancy_grid')}\n\n")
    except Exception as e:
        logging.warning(f"Error while logging market progression {e}")


domain_name_from_env = os.environ.get("API_CLIENT_DOMAIN_NAME", DEFAULT_DOMAIN_NAME)


websocket_domain_name_from_env = os.environ.get("API_CLIENT_WEBSOCKET_DOMAIN_NAME", DEFAULT_WEBSOCKET_DOMAIN)

consumer_websocket_domain_name_from_env = os.environ.get("CONSUMER_WEBSOCKET_DOMAIN_NAME", CONSUMER_WEBSOCKET_DOMAIN_NAME)


def log_bid_offer_confirmation(message):
    try:
        if message.get("status") == "ready":
            event = message.get("command")
            data_dict = json.loads(message.get(event))
            energy = data_dict.get("energy")
            price = data_dict.get("price")
            trader = data_dict.get("seller" if event=="offer" else "buyer")
            logging.info(f"{trader} {'OFFERED' if event == 'offer' else 'BID'} "
                         f"{round(energy, 2)} kWh at {price} cts/kWh")
    except Exception as e:
        logging.error(f"Logging bid/offer info failed.{e}")
