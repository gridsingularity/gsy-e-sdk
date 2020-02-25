from d3a_api_client import APIClientInterface
import os
import logging
import requests
import json
import ssl
from functools import wraps
from concurrent.futures.thread import ThreadPoolExecutor
from d3a_api_client.websocket_device import WebsocketMessageDispatcher, WebsocketThread


root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)


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


class RestDeviceClient(APIClientInterface):

    def __init__(self, simulation_id, device_id, domain_name,
                 websockets_domain_name, is_ssl, autoregister=False):
        self.simulation_id = simulation_id
        self.device_id = device_id
        self.domain_name = domain_name
        self.retrieve_key_from_server()

        if is_ssl:
            self.pem_cert = ssl.get_server_certificate(domain_name)
        self.dispatcher = WebsocketMessageDispatcher(self)
        self.websocket_thread = WebsocketThread(simulation_id, device_id, self.jwt_token,
                                                websockets_domain_name, self.dispatcher)
        self.websocket_thread.start()
        self.callback_thread = ThreadPoolExecutor(max_workers=5)
        self.registered = False
        if autoregister:
            print(f"Trying to register")
            self.register()

    def retrieve_key_from_server(self):
        resp = requests.post(
            f"{self.domain_name}/api-token-auth/",
            data=json.dumps({"username": os.environ["API_CLIENT_USERNAME"],
                             "password": os.environ["API_CLIENT_PASSWORD"]}),
            headers={"Content-Type": "application/json"})
        if resp.status_code != 200:
            logging.error(f"Request for token authentication failed with status code {resp.status_code}."
                          f"Response body: {resp.text}")
            return
        self.jwt_token = json.loads(resp.text)["token"]

    @property
    def _url_prefix(self):
        return f'{self.domain_name}/external-connection/api/{self.simulation_id}/{self.device_id}/'

    def post_request(self, endpoint_suffix, data):
        resp = requests.post(
            f"{self._url_prefix}/{endpoint_suffix}/",
            data=json.dumps(data),
            headers={"Content-Type": "application/json",
                     "Authorization": f"JWT {self.jwt_token}"})
        if resp.status_code != 200:
            logging.error(f"Request {endpoint_suffix} failed with status code {resp.status_code}."
                          f"Response body: {resp.text}")
            return False
        return True

    def get_request(self, endpoint_suffix):
        resp = requests.post(
            f"{self._url_prefix}/{endpoint_suffix}/",
            headers={"Content-Type": "application/json",
                     "Authorization": f"JWT {self.jwt_token}"})
        if resp.status_code != 200:
            logging.error(f"Request {endpoint_suffix} failed with status code {resp.status_code}."
                          f"Response body: {resp.text}")
            return False
        return True

    @logging_decorator('register')
    def register(self, is_blocking=True):
        if self.post_request('register', {}):
            return_value = self.dispatcher.wait_for_command_response('register')
            self.registered = True
            return return_value

    @logging_decorator('unregister')
    def unregister(self, is_blocking):
        if self.post_request('unregister', {}):
            return_value = self.dispatcher.wait_for_command_response('unregister')
            self.registered = False
            return return_value

    @logging_decorator('offer')
    def offer_energy(self, energy, price):
        if self.post_request('offer', {"energy": energy, "price": price}):
            return self.dispatcher.wait_for_command_response('offer')

    @logging_decorator('bid')
    def bid_energy(self, energy, price):
        if self.post_request('bid', {"energy": energy, "price": price}):
            return self.dispatcher.wait_for_command_response('bid')

    @logging_decorator('delete offer')
    def delete_offer(self, offer_id):
        if self.post_request('delete-offer', {"offer": offer_id}):
            return self.dispatcher.wait_for_command_response('offer_delete')

    @logging_decorator('delete bid')
    def delete_bid(self, bid_id):
        if self.post_request('delete-bid', {"bid": bid_id}):
            return self.dispatcher.wait_for_command_response('bid_delete')

    @logging_decorator('list offers')
    def list_offers(self):
        if self.get_request('offers'):
            return self.dispatcher.wait_for_command_response('offers')

    @logging_decorator('list bids')
    def list_bids(self):
        if self.get_request('bids'):
            return self.dispatcher.wait_for_command_response('bids')

    def on_register(self, registration_info):
        pass

    def _on_market_cycle(self, message):
        logging.debug(f"A new market was created. Market information: {message}")

        def executor_function():
            self.on_market_cycle(message)
        self.callback_thread.submit(executor_function)

    def _on_tick(self, message):
        logging.debug(f"Time has elapsed on the device. Progress info: {message}")

        def executor_function():
            self.on_tick(message)
        self.callback_thread.submit(executor_function)

    def _on_trade(self, message):
        logging.debug(f"A trade took place on the device. Trade information: {message}")

        def executor_function():
            self.on_trade(message)
        self.callback_thread.submit(executor_function)

    def on_market_cycle(self, market_info):
        if not self.registered:
            self.register()

    def on_tick(self, tick_info):
        pass

    def on_trade(self, trade_info):
        pass
