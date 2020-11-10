import logging
import traceback

from threading import Lock
from concurrent.futures.thread import ThreadPoolExecutor

from d3a_api_client import APIClientInterface
from d3a_api_client.websocket_device import WebsocketMessageReceiver, WebsocketThread
from d3a_api_client.utils import retrieve_jwt_key_from_server, RestCommunicationMixin, \
    logging_decorator, get_aggregator_prefix, blocking_post_request
from d3a_api_client.constants import MAX_WORKER_THREADS
from d3a_interface.utils import RepeatingTimer
from d3a_interface.constants_limits import JWT_TOKEN_EXPIRY_IN_SECS

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

REGISTER_COMMAND_TIMEOUT = 15*60


class RestDeviceClient(APIClientInterface, RestCommunicationMixin):

    def __init__(self, simulation_id, device_id, domain_name,
                 websockets_domain_name, autoregister=False, start_websocket=True,
                 sim_api_domain_name=None):
        self.simulation_id = simulation_id
        self.device_id = device_id
        self.domain_name = domain_name
        if sim_api_domain_name is None:
            sim_api_domain_name = self.domain_name
        self.jwt_token = retrieve_jwt_key_from_server(sim_api_domain_name)
        self.websockets_domain_name = websockets_domain_name
        self.aggregator_prefix = get_aggregator_prefix(domain_name, simulation_id)
        self.active_aggregator = None
        self.lock = Lock()
        self.jwt_token_refresh = RepeatingTimer(
            JWT_TOKEN_EXPIRY_IN_SECS - 30, self.refresh_jwt_token, [sim_api_domain_name]
        )
        self.jwt_token_refresh.start()
        if start_websocket:
            self.start_websocket_connection()

        self.registered = False
        if autoregister:
            self.register()

    def refresh_jwt_token(self, domain_name):
        with self.lock:
            self.jwt_token = retrieve_jwt_key_from_server(domain_name)

    def start_websocket_connection(self):
        self.dispatcher = WebsocketMessageReceiver(self)
        self.websocket_thread = WebsocketThread(self.simulation_id, self.device_id, self.jwt_token,
                                                self.websockets_domain_name, self.dispatcher)
        self.websocket_thread.start()
        self.callback_thread = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)

    @logging_decorator('register')
    def register(self, is_blocking=True):
        transaction_id, posted = self._post_request('register', {})
        if posted:
            return_value = self.dispatcher.wait_for_command_response(
                'register', transaction_id, timeout=REGISTER_COMMAND_TIMEOUT)
            self.registered = return_value["registered"]
            return return_value

    @logging_decorator('unregister')
    def unregister(self, is_blocking):
        transaction_id, posted = self._post_request('unregister', {})
        if posted:
            return_value = self.dispatcher.wait_for_command_response(
                'unregister', transaction_id, timeout=REGISTER_COMMAND_TIMEOUT)
            self.registered = False
            return return_value

    @logging_decorator('select-aggregator')
    def select_aggregator(self, aggregator_uuid):
        response = blocking_post_request(f'{self.aggregator_prefix}select-aggregator/',
                                         {"aggregator_uuid": aggregator_uuid,
                                          "device_uuid": self.device_id}, self.jwt_token)
        self.active_aggregator = response["aggregator_uuid"]

    @logging_decorator('unselect-aggregator')
    def unselect_aggregator(self, aggregator_uuid):
        response = blocking_post_request(f'{self.aggregator_prefix}unselect-aggregator/',
                                         {"aggregator_uuid": aggregator_uuid,
                                          "device_uuid": self.device_id}, self.jwt_token)
        self.active_aggregator = None

    @logging_decorator('set_power_forecast')
    def set_power_forecast(self, pv_power_forecast_W, do_not_wait=False):
        transaction_id, posted = self._post_request('set_power_forecast',
                                                    {"power_forecast": pv_power_forecast_W})
        if posted and do_not_wait is False:
            return self.dispatcher.wait_for_command_response('set_power_forecast', transaction_id)

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
        transaction_id, get_sent = self._get_request('list-offers', {})
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
            try:
                self.on_market_cycle(message)
            except Exception as e:
                logging.error(f"on_market_cycle raised exception): {e}. \n Traceback: {traceback.format_exc()}")
        self.callback_thread.submit(executor_function)

    def _on_tick(self, message):
        logging.debug(f"Time has elapsed on the device. Progress info: {message}")

        def executor_function():
            try:
                self.on_tick(message)
            except Exception as e:
                logging.error(f"on_tick raised exception: {e}. \n Traceback: {traceback.format_exc()}")
        self.callback_thread.submit(executor_function)

    def _on_trade(self, message):
        logging.debug(f"A trade took place on the device. Trade information: {message}")

        def executor_function():
            try:
                self.on_trade(message)
            except Exception as e:
                logging.error(f"on_trade raised exception: {e}. \n Traceback: {traceback.format_exc()}")
        self.callback_thread.submit(executor_function)

    def _on_finish(self, message):
        logging.debug(f"Simulation finished. Information: {message}")

        def executor_function():
            try:
                self.on_finish(message)
            except Exception as e:
                logging.error(f"on_finish raised exception: {e}. \n Traceback: {traceback.format_exc()}")
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