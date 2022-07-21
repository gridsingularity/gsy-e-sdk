import logging
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Dict

from gsy_framework.client_connections.utils import (
    RestCommunicationMixin, blocking_post_request, log_market_progression,
    retrieve_jwt_key_from_server)
from gsy_framework.client_connections.websocket_connection import WebsocketThread
from gsy_framework.utils import execute_function_util

from gsy_e_sdk import APIClientInterface
from gsy_e_sdk.constants import MAX_WORKER_THREADS
from gsy_e_sdk.utils import (
    domain_name_from_env, get_aggregator_prefix, get_configuration_prefix, log_trade_info,
    logging_decorator, simulation_id_from_env, websocket_domain_name_from_env)
from gsy_e_sdk.websocket_device import DeviceWebsocketMessageReceiver


REGISTER_COMMAND_TIMEOUT = 15 * 60


# pylint: disable-next=too-many-instance-attributes
class RestAssetClient(APIClientInterface, RestCommunicationMixin):
    """Client class for assets to be used while working with REST."""

    # pylint: disable-next=super-init-not-called
    # pylint: disable-next=too-many-arguments
    def __init__(
            self, asset_uuid, simulation_id=None, domain_name=None, websockets_domain_name=None,
            autoregister=False, start_websocket=True, sim_api_domain_name=None):
        self.simulation_id = simulation_id if simulation_id else simulation_id_from_env()
        self.domain_name = domain_name if domain_name else domain_name_from_env()
        self.websockets_domain_name = websockets_domain_name or websocket_domain_name_from_env()
        self.asset_uuid = asset_uuid
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
        """Return the prefix of the URL used to connect to the asset's endpoints."""
        return f"{self.domain_name}/external-connection/api/{self.simulation_id}/{self.asset_uuid}"

    def start_websocket_connection(self):
        """Initiate the websocket connection to the exchange."""
        self.dispatcher = DeviceWebsocketMessageReceiver(self)
        websocket_uri = f"{self.websockets_domain_name}/{self.simulation_id}/{self.asset_uuid}/"
        self.websocket_thread = WebsocketThread(websocket_uri, self.domain_name, self.dispatcher)
        self.websocket_thread.start()
        self.callback_thread = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)

    @logging_decorator("register")
    def register(self, is_blocking=True):
        """Register the asset with the exchange."""
        transaction_id, posted = self._post_request(f"{self.endpoint_prefix}/register", {})
        if posted:
            return_value = self.dispatcher.wait_for_command_response(
                "register", transaction_id, timeout=REGISTER_COMMAND_TIMEOUT)
            self.registered = return_value["registered"]
            return return_value

        return None

    @logging_decorator("unregister")
    def unregister(self, is_blocking):
        """Unregister the asset from the exchange."""
        transaction_id, posted = self._post_request(f"{self.endpoint_prefix}/unregister", {})
        if posted:
            return_value = self.dispatcher.wait_for_command_response(
                "unregister", transaction_id, timeout=REGISTER_COMMAND_TIMEOUT)
            self.registered = False
            return return_value

        return None

    @logging_decorator("select-aggregator")
    def select_aggregator(self, aggregator_uuid):
        """Connect the asset with its aggregator (identified by the provided ID)."""
        response = blocking_post_request(
            f"{self.aggregator_prefix}select-aggregator/",
            {"aggregator_uuid": aggregator_uuid, "device_uuid": self.asset_uuid},
            self.jwt_token)

        self.active_aggregator = response["aggregator_uuid"] if response else None

    @logging_decorator("unselect-aggregator")
    def unselect_aggregator(self, aggregator_uuid):
        """Disconnect the asset from its aggregator (identified by the provided ID)."""
        blocking_post_request(
            f"{self.aggregator_prefix}unselect-aggregator/",
            {"aggregator_uuid": aggregator_uuid, "device_uuid": self.asset_uuid},
            self.jwt_token)

        self.active_aggregator = None

    # pylint: disable=invalid-name
    @logging_decorator("set-energy-forecast")
    def set_energy_forecast(self, energy_forecast_kWh: Dict, do_not_wait=False):
        """Communicate the energy forecast of the asset to the exchange."""
        transaction_id, posted = self._post_request(f"{self.endpoint_prefix}/set-energy-forecast",
                                                    {"energy_forecast": energy_forecast_kWh})
        if posted and do_not_wait is False:
            return self.dispatcher.wait_for_command_response("set_energy_forecast", transaction_id)

        return None

    # pylint: disable=invalid-name
    @logging_decorator("set-energy-measurement")
    def set_energy_measurement(self, energy_measurement_kWh: Dict, do_not_wait=False):
        """Communicate the energy measurement of the asset to the exchange."""
        transaction_id, posted = self._post_request(
            f"{self.endpoint_prefix}/set-energy-measurement",
            {"energy_measurement": energy_measurement_kWh})
        if posted and do_not_wait is False:
            return self.dispatcher.wait_for_command_response("set_energy_measurement",
                                                             transaction_id)
        return None

    def _on_event_or_response(self, message):
        logging.debug("A new message was received. Message information: %s", message)
        log_market_progression(message)
        self.callback_thread.submit(execute_function_util,
                                    function=lambda: self.on_event_or_response(message),
                                    function_name="on_event_or_response")

    def _on_market_cycle(self, message):
        self.callback_thread.submit(execute_function_util,
                                    function=lambda: self.on_market_slot(message),
                                    function_name="on_market_slot")

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

    # pylint: disable=unused-argument
    def on_market_cycle(self, market_info):
        """Perform actions that should be triggered on market_cycle event."""
        if not self.registered:
            self.register()

    def on_market_slot(self, market_info):
        self.on_market_cycle(market_info)

    def on_tick(self, tick_info):
        """Perform actions that should be triggered on tick event."""

    def on_trade(self, trade_info):
        """Perform actions that should be triggered on on_trade event."""

    def on_finish(self, finish_info):
        """Perform actions that should be triggered on on_finish event."""
