from d3a_api_client import APIClientInterface
import logging
import requests
from functools import wraps
from concurrent.futures.thread import ThreadPoolExecutor
from d3a_api_client.websocket_device import WebsocketMessageReceiver, WebsocketThread
from d3a_api_client.utils import retrieve_jwt_key_from_server, post_request, get_request


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
                 websockets_domain_name, autoregister=False):
        self.simulation_id = simulation_id
        self.device_id = device_id
        self.domain_name = domain_name
        self.jwt_token = retrieve_jwt_key_from_server(domain_name)

        self.dispatcher = WebsocketMessageReceiver(self)
        self.websocket_thread = WebsocketThread(simulation_id, device_id, self.jwt_token,
                                                websockets_domain_name, self.dispatcher)
        self.websocket_thread.start()
        self.callback_thread = ThreadPoolExecutor(max_workers=5)
        self.registered = False
        if autoregister:
            self.register()

    @property
    def _url_prefix(self):
        return f'{self.domain_name}/external-connection/api/{self.simulation_id}/{self.device_id}'

    def _post_request(self, endpoint_suffix, data):
        return post_request(f"{self._url_prefix}/{endpoint_suffix}/", data, self.jwt_token)

    def _get_request(self, endpoint_suffix):
        return get_request(f"{self._url_prefix}/{endpoint_suffix}/", self.jwt_token)

    @logging_decorator('register')
    def register(self, is_blocking=True):
        if self._post_request('register', {}):
            return_value = self.dispatcher.wait_for_command_response('register')
            self.registered = return_value["registered"]
            return return_value

    @logging_decorator('unregister')
    def unregister(self, is_blocking):
        if self._post_request('unregister', {}):
            return_value = self.dispatcher.wait_for_command_response('unregister')
            self.registered = False
            return return_value

    @logging_decorator('offer')
    def offer_energy(self, energy, price):
        if self._post_request('offer', {"energy": energy, "price": price}):
            return self.dispatcher.wait_for_command_response('offer')

    @logging_decorator('bid')
    def bid_energy(self, energy, price):
        if self._post_request('bid', {"energy": energy, "price": price}):
            return self.dispatcher.wait_for_command_response('bid')

    @logging_decorator('delete offer')
    def delete_offer(self, offer_id):
        if self._post_request('delete-offer', {"offer": offer_id}):
            return self.dispatcher.wait_for_command_response('offer_delete')

    @logging_decorator('delete bid')
    def delete_bid(self, bid_id):
        if self._post_request('delete-bid', {"bid": bid_id}):
            return self.dispatcher.wait_for_command_response('bid_delete')

    @logging_decorator('list offers')
    def list_offers(self):
        if self._get_request('list-offers'):
            return self.dispatcher.wait_for_command_response('list_offers')

    @logging_decorator('list bids')
    def list_bids(self):
        if self._get_request('list-bids'):
            return self.dispatcher.wait_for_command_response('list_bids')

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
