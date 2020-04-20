from d3a_api_client import APIClientInterface
import logging
from functools import wraps
from concurrent.futures.thread import ThreadPoolExecutor
from d3a_api_client.websocket_device import WebsocketMessageReceiver, WebsocketThread
from d3a_api_client.utils import retrieve_jwt_key_from_server, RestCommunicationMixin
from d3a_api_client.constants import MAX_WORKER_THREADS

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


class RestDeviceClient(APIClientInterface, RestCommunicationMixin):

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
        self.callback_thread = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)
        self.registered = False
        if autoregister:
            self.register()

    @logging_decorator('register')
    def register(self, is_blocking=True):
        transaction_id, posted = self._post_request('register', {})
        if posted:
            return_value = self.dispatcher.wait_for_command_response('register', transaction_id)
            self.registered = return_value["registered"]
            return return_value

    @logging_decorator('unregister')
    def unregister(self, is_blocking):
        transaction_id, posted = self._post_request('unregister', {})
        if posted:
            return_value = self.dispatcher.wait_for_command_response('unregister', transaction_id)
            self.registered = False
            return return_value

    @logging_decorator('offer')
    def offer_energy(self, energy, price):
        transaction_id, posted = self._post_request('offer', {"energy": energy, "price": price})
        if posted:
            return self.dispatcher.wait_for_command_response('offer', transaction_id)

    @logging_decorator('offer')
    def offer_energy_rate(self, energy, rate):
        transaction_id, posted = self._post_request(
            'offer', {"energy": energy, "price": rate * energy})
        if posted:
            return self.dispatcher.wait_for_command_response('offer', transaction_id)

    @logging_decorator('bid')
    def bid_energy(self, energy, price):
        transaction_id, posted = self._post_request('bid', {"energy": energy, "price": price})
        if posted:
            return self.dispatcher.wait_for_command_response('bid', transaction_id)

    @logging_decorator('bid')
    def bid_energy_rate(self, energy, rate):
        transaction_id, posted = self._post_request(
            'bid', {"energy": energy, "price": rate * energy})
        if posted:
            return self.dispatcher.wait_for_command_response('bid', transaction_id)

    @logging_decorator('delete offer')
    def delete_offer(self, offer_id=None):
        transaction_id, posted = self._post_request('delete-offer', {"offer": offer_id})
        if posted:
            return self.dispatcher.wait_for_command_response('offer_delete', transaction_id)

    @logging_decorator('delete bid')
    def delete_bid(self, bid_id=None):
        transaction_id, posted = self._post_request('delete-bid', {"bid": bid_id})
        if posted:
            return self.dispatcher.wait_for_command_response('bid_delete', transaction_id)

    @logging_decorator('list offers')
    def list_offers(self):
        transaction_id, get_sent = self._get_request('list-offers', "")
        if get_sent:
            return self.dispatcher.wait_for_command_response('list_offers', transaction_id)

    @logging_decorator('list bids')
    def list_bids(self):
        transaction_id, get_sent = self._get_request('list-bids', {})
        if get_sent:
            return self.dispatcher.wait_for_command_response('list_bids', transaction_id)

    @logging_decorator('device info')
    def device_info(self):
        transaction_id, get_sent = self._get_request('device-stats', {})
        if get_sent:
            return self.dispatcher.wait_for_command_response('device_info', transaction_id)

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

    def _on_finish(self, message):
        logging.debug(f"Simulation finished. Information: {message}")

        def executor_function():
            self.on_finish(message)
        self.callback_thread.submit(executor_function)

    def on_market_cycle(self, market_info):
        if not self.registered:
            self.register()

    def on_tick(self, tick_info):
        pass

    def on_trade(self, trade_info):
        pass

    def on_finish(self, finish_info):
        pass