from concurrent.futures.thread import ThreadPoolExecutor

from d3a_api_client.constants import MAX_WORKER_THREADS
from d3a_api_client.utils import domain_name_from_env, websocket_domain_name_from_env, \
    simulation_id_from_env
from d3a_api_client.utils import retrieve_jwt_key_from_server, RestCommunicationMixin, \
    logging_decorator, blocking_post_request, get_aggregator_prefix
from d3a_api_client.websocket_device import WebsocketMessageReceiver, WebsocketThread


class RestMarketClient(RestCommunicationMixin):

    def __init__(self, area_id, simulation_id=None, domain_name=None, websockets_domain_name=None):
        self.area_id = area_id
        self.simulation_id = simulation_id if simulation_id else simulation_id_from_env()
        self.domain_name = domain_name if domain_name else domain_name_from_env()
        self.websockets_domain_name = websockets_domain_name \
            if websockets_domain_name else websocket_domain_name_from_env()
        self.jwt_token = retrieve_jwt_key_from_server(self.domain_name)
        self._create_jwt_refresh_timer(self.domain_name)

        self.start_websocket_connection()
        self.aggregator_prefix = get_aggregator_prefix(self.domain_name, self.simulation_id)
        self.active_aggregator = None

    def start_websocket_connection(self):
        self.dispatcher = WebsocketMessageReceiver(self)
        self.websocket_thread = WebsocketThread(self.simulation_id, self.area_id,
                                                self.websockets_domain_name, self.domain_name,
                                                self.dispatcher)
        self.websocket_thread.start()
        self.callback_thread = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)

    @logging_decorator('select-aggregator')
    def select_aggregator(self, aggregator_uuid):
        response = blocking_post_request(f'{self.aggregator_prefix}select-aggregator/',
                                         {"aggregator_uuid": aggregator_uuid,
                                          "device_uuid": self.area_id}, self.jwt_token)
        self.active_aggregator = response["aggregator_uuid"] if response else None

    @logging_decorator('unselect-aggregator')
    def unselect_aggregator(self, aggregator_uuid):
        response = blocking_post_request(f'{self.aggregator_prefix}unselect-aggregator/',
                                         {"aggregator_uuid": aggregator_uuid,
                                          "device_uuid": self.area_id}, self.jwt_token)
        self.active_aggregator = None

    @logging_decorator('grid_fees')
    def grid_fees(self, fee_cents_per_kWh):
        transaction_id, get_sent = self._post_request('grid-fee', {"fee_const": fee_cents_per_kWh})
        if get_sent:
            return self.dispatcher.wait_for_command_response('grid_fees', transaction_id)

    @logging_decorator('dso_market_stats')
    def last_market_dso_stats(self):
        transaction_id, posted = self._get_request('dso-market-stats', {})
        if posted:
            return self.dispatcher.wait_for_command_response('dso_market_stats', transaction_id)
