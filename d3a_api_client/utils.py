import os
import requests
import json
import logging


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
    return get_area_uuid_from_area_name(
        json.loads(data["data"]["readConfiguration"]["scenarioData"]["representation"]["serialized"]), area_name
    )
