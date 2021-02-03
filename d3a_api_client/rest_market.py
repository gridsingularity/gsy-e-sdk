import logging

from concurrent.futures.thread import ThreadPoolExecutor
from d3a_api_client.websocket_device import WebsocketMessageReceiver, WebsocketThread
from d3a_api_client.utils import retrieve_jwt_key_from_server, RestCommunicationMixin, \
    logging_decorator, blocking_post_request, get_aggregator_prefix, execute_function_util, log_market_progression
from d3a_api_client.constants import MAX_WORKER_THREADS
from d3a_api_client.utils import domain_name_from_env, websocket_domain_name_from_env


class RestMarketClient(RestCommunicationMixin):

    def __init__(self, simulation_id, area_id, domain_name=domain_name_from_env,
                 websockets_domain_name=websocket_domain_name_from_env):
        self.simulation_id = simulation_id
        self.device_id = area_id
        self.domain_name = domain_name
        self.jwt_token = retrieve_jwt_key_from_server(domain_name)
        self._create_jwt_refresh_timer(domain_name)
        self.dispatcher = WebsocketMessageReceiver(self)
        self.websocket_thread = WebsocketThread(simulation_id, area_id, self.jwt_token,
                                                websockets_domain_name, self.dispatcher)
        self.websocket_thread.start()
        self.callback_thread = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)
        self.aggregator_prefix = get_aggregator_prefix(domain_name, simulation_id)
        self.active_aggregator = None

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

    @logging_decorator('market_stats')
    def last_market_stats(self):
        transaction_id, posted = self._get_request('market-stats', {})
        if posted:
            return self.dispatcher.wait_for_command_response('market_stats', transaction_id)

    @logging_decorator('grid_fees')
    def grid_fees(self, fee_cents_per_kWh):
        transaction_id, get_sent = self._post_request('grid-fee', {"fee_const": fee_cents_per_kWh})
        if get_sent:
            return self.dispatcher.wait_for_command_response('grid_fees', transaction_id)

    @logging_decorator('dso_market_stats')
    def list_dso_market_stats(self, selected_markets):
        transaction_id, posted = self._get_request('dso-market-stats', {"market_slots": selected_markets})
        if posted:
            return self.dispatcher.wait_for_command_response('dso_market_stats', transaction_id)

    def _on_event_or_response(self, message):
        logging.info(f"A new message was received. Message information: {message}")
        log_market_progression(message)
        function = lambda: self.on_event_or_response(message)
        self.callback_thread.submit(execute_function_util, function=function,
                                    function_name="on_event_or_response")

    def _on_market_cycle(self, message):
        function = lambda: self.on_market_cycle(message)
        self.callback_thread.submit(execute_function_util, function=function,
                                    function_name="on_market_cycle")

    def _on_finish(self, message):
        function = lambda: self.on_finish(message)
        self.callback_thread.submit(execute_function_util, function=function,
                                    function_name="on_finish")

    def on_finish(self, finish_info):
        pass

    def on_market_cycle(self, market_info):
        pass

    def on_event_or_response(self, message):
        pass
