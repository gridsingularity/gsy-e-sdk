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


def get_request(endpoint, jwt_token):
    resp = requests.get(
        endpoint,
        headers={"Content-Type": "application/json",
                 "Authorization": f"JWT {jwt_token}"})
    if resp.status_code != 200:
        logging.error(f"Request to {endpoint} failed with status code {resp.status_code}."
                      f"Response body: {resp.text}")
        return False
    return True
