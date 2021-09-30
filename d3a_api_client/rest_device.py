import logging
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Dict

from d3a_interface.client_connections.utils import (
    RestCommunicationMixin, retrieve_jwt_key_from_server, blocking_post_request)
from d3a_interface.client_connections.utils import log_market_progression
from d3a_interface.client_connections.websocket_connection import WebsocketThread
from d3a_interface.utils import execute_function_util

from d3a_api_client import APIClientInterface
from d3a_api_client.constants import MAX_WORKER_THREADS
from d3a_api_client.utils import (
    logging_decorator, get_aggregator_prefix,
    domain_name_from_env, websocket_domain_name_from_env,
    simulation_id_from_env, get_configuration_prefix, log_trade_info)
from d3a_api_client.websocket_device import DeviceWebsocketMessageReceiver

REGISTER_COMMAND_TIMEOUT = 15 * 60


class RestDeviceClient(APIClientInterface, RestCommunicationMixin):

    def __init__(self, area_id, simulation_id=None, domain_name=None, websockets_domain_name=None,
                 autoregister=False, start_websocket=True, sim_api_domain_name=None):
        self.simulation_id = simulation_id if simulation_id else simulation_id_from_env()
        self.domain_name = domain_name if domain_name else domain_name_from_env()
        self.websockets_domain_name = websockets_domain_name \
            if websockets_domain_name else websocket_domain_name_from_env()
        self.area_id = area_id
        if sim_api_domain_name is None:
            sim_api_domain_name = self.domain_name
        self.jwt_token = retrieve_jwt_key_from_server(sim_api_domain_name)
        self._create_jwt_refresh_timer(sim_api_domain_name)
        self.aggregator_prefix = get_aggregator_prefix(self.domain_name, self.simulation_id)
        self.configuration_prefix = get_configuration_prefix(self.domain_name, self.simulation_id)
        self.active_aggregator = None

        if start_websocket or autoregister:
            self.start_websocket_connection()

        self.registered = False
        if autoregister:
            self.register()

    @property
    def endpoint_prefix(self):
        return f"{self.domain_name}/external-connection/api/{self.simulation_id}/{self.area_id}"

    def start_websocket_connection(self):
        self.dispatcher = DeviceWebsocketMessageReceiver(self)
        websocket_uri = f"{self.websockets_domain_name}/{self.simulation_id}/{self.area_id}/"
        self.websocket_thread = WebsocketThread(websocket_uri, self.domain_name,
                                                self.dispatcher)
        self.websocket_thread.start()
        self.callback_thread = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)

    @logging_decorator("register")
    def register(self, is_blocking=True):
        transaction_id, posted = self._post_request(f"{self.endpoint_prefix}/register", {})
        if posted:
            return_value = self.dispatcher.wait_for_command_response(
                "register", transaction_id, timeout=REGISTER_COMMAND_TIMEOUT)
            self.registered = return_value["registered"]
            return return_value

    @logging_decorator("unregister")
    def unregister(self, is_blocking):
        transaction_id, posted = self._post_request(f"{self.endpoint_prefix}/unregister", {})
        if posted:
            return_value = self.dispatcher.wait_for_command_response(
                "unregister", transaction_id, timeout=REGISTER_COMMAND_TIMEOUT)
            self.registered = False
            return return_value

    @logging_decorator("select-aggregator")
    def select_aggregator(self, aggregator_uuid):
        response = blocking_post_request(f"{self.aggregator_prefix}select-aggregator/",
                                         {"aggregator_uuid": aggregator_uuid,
                                          "device_uuid": self.area_id}, self.jwt_token)
        self.active_aggregator = response["aggregator_uuid"] if response else None

    @logging_decorator("unselect-aggregator")
    def unselect_aggregator(self, aggregator_uuid):
        response = blocking_post_request(f"{self.aggregator_prefix}unselect-aggregator/",
                                         {"aggregator_uuid": aggregator_uuid,
                                          "device_uuid": self.area_id}, self.jwt_token)
        self.active_aggregator = None

    @logging_decorator("set-energy-forecast")
    def set_energy_forecast(self, energy_forecast_kWh: Dict, do_not_wait=False):
        transaction_id, posted = self._post_request(f"{self.endpoint_prefix}/set-energy-forecast",
                                                    {"energy_forecast": energy_forecast_kWh})
        if posted and do_not_wait is False:
            return self.dispatcher.wait_for_command_response("set_energy_forecast", transaction_id)

    @logging_decorator("set-energy-measurement")
    def set_energy_measurement(self, energy_measurement_kWh: Dict, do_not_wait=False):
        transaction_id, posted = self._post_request(
            f"{self.endpoint_prefix}/set-energy-measurement",
            {"energy_measurement": energy_measurement_kWh})
        if posted and do_not_wait is False:
            return self.dispatcher.wait_for_command_response("set_energy_measurement",
                                                             transaction_id)

    def _on_event_or_response(self, message):
        logging.debug(f"A new message was received. Message information: {message}")
        log_market_progression(message)
        self.callback_thread.submit(execute_function_util,
                                    function=lambda: self.on_event_or_response(message),
                                    function_name="on_event_or_response")

    def _on_market_cycle(self, message):
        self.callback_thread.submit(execute_function_util,
                                    function=lambda: self.on_market_cycle(message),
                                    function_name="on_market_cycle")

    def _on_tick(self, message):
        self.callback_thread.submit(execute_function_util,
                                    function=lambda: self.on_tick(message),
                                    function_name="on_tick")

    def _on_trade(self, message):
        for individual_trade in message["trade_list"]:
            log_trade_info(individual_trade)

        self.callback_thread.submit(execute_function_util,
                                    function=lambda: self.on_trade(message),
                                    function_name="on_trade")

    def _on_finish(self, message):
        self.callback_thread.submit(execute_function_util,
                                    function=lambda: self.on_finish(message),
                                    function_name="on_finish")

    def on_market_cycle(self, market_info):
        if not self.registered:
            self.register()

    def on_tick(self, tick_info):
        pass

    def on_trade(self, trade_info):
        pass

    def on_finish(self, finish_info):
        pass
