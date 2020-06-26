import os
import requests
import json
import logging
import uuid
from functools import wraps


class AreaNotFoundException(Exception):
    pass


class RestWebsocketAPIException(Exception):
    pass


class RestCommunicationMixin:

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


def retrieve_jwt_key_from_server(domain_name):
    resp = requests.post(
        f"{domain_name}/api-token-auth/",
        data=json.dumps({"username": os.environ["API_CLIENT_USERNAME"],
                         "password": os.environ["API_CLIENT_PASSWORD"]}),
        headers={"Content-Type": "application/json"})
    if resp.status_code != 200:
        logging.error(f"Request for token authentication failed with status code {resp.status_code}."
                      f"Response body: {resp.text}")
        return
    return json.loads(resp.text)["token"]


def post_request(endpoint, data, jwt_token):
    resp = requests.post(
        endpoint,
        data=json.dumps(data),
        headers={"Content-Type": "application/json",
                 "Authorization": f"JWT {jwt_token}"})
    if resp.status_code != 200:
        logging.error(f"Request to {endpoint} failed with status code {resp.status_code}."
                      f"Response body: {resp.text} {resp.reason}")
        return False
    return True


def blocking_post_request(endpoint, data, jwt_token):
    data["transaction_id"] = str(uuid.uuid4())
    response = requests.post(
        endpoint,
        data=json.dumps(data),
        headers={"Content-Type": "application/json",
                 "Authorization": f"JWT {jwt_token}"})
    return json.loads(response.json())


def get_request(endpoint, data, jwt_token):
    resp = requests.get(
        endpoint,
        data=json.dumps(data),
        headers={"Content-Type": "application/json",
                 "Authorization": f"JWT {jwt_token}"})
    if resp.status_code != 200:
        logging.error(f"Request to {endpoint} failed with status code {resp.status_code}."
                      f"Response body: {resp.text}")
        return False
    return True


def get_aggregator_prefix(domain_name, simulation_id):
    return f"{domain_name}/external-connection/aggregator-api/{simulation_id}/"


def blocking_get_request(endpoint, data, jwt_token):
    data["transaction_id"] = str(uuid.uuid4())
    response = requests.get(
        endpoint,
        data=json.dumps(data),
        headers={"Content-Type": "application/json",
                 "Authorization": f"JWT {jwt_token}"})
    print(f"response: {response}")
    return json.loads(response.json())


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
    jwt_key = retrieve_jwt_key_from_server(domain_name)
    from sgqlc.endpoint.http import HTTPEndpoint

    url = f"{domain_name}/graphql/"
    headers = {'Authorization': f'JWT {jwt_key}', 'Content-Type': 'application/json'}

    query = 'query { readConfiguration(uuid: "{' + collab_id + \
            '}") { scenarioData { representation { serialized } } } }'

    endpoint = HTTPEndpoint(url, headers)
    data = endpoint(query=query)
    area_uuid = get_area_uuid_from_area_name(
        json.loads(data["data"]["readConfiguration"]["scenarioData"]["representation"]["serialized"]), area_name
    )
    if not area_uuid:
        raise AreaNotFoundException(f"Area with name {area_name} is not part of the "
                                    f"collaboration with UUID {collab_id}")
    return area_uuid


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
