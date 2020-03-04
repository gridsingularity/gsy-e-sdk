import logging
from d3a_api_client.rest_device import logging_decorator
from concurrent.futures.thread import ThreadPoolExecutor
from d3a_api_client.websocket_device import WebsocketMessageReceiver, WebsocketThread
from d3a_api_client.utils import retrieve_jwt_key_from_server


root_logger = logging.getLogger()
root_logger.setLevel(logging.ERROR)


class RestMarketClient:

    def __init__(self, simulation_id, device_id, domain_name,
                 websockets_domain_name):
        self.simulation_id = simulation_id
        self.device_id = device_id
        self.domain_name = domain_name
        self.jwt_token = retrieve_jwt_key_from_server(domain_name)

        self.dispatcher = WebsocketMessageReceiver(self)
        self.websocket_thread = WebsocketThread(simulation_id, device_id, self.jwt_token,
                                                websockets_domain_name, self.dispatcher)
        self.websocket_thread.start()
        self.callback_thread = ThreadPoolExecutor(max_workers=5)

    @property
    def _url_prefix(self):
        return f'{self.domain_name}/external-connection/api/{self.simulation_id}/{self.device_id}'

    @logging_decorator('market_stats')
    def list_stats(self):
        if self._post_request('market_stats', {}):
            return self.dispatcher.wait_for_command_response('market_stats')

    @logging_decorator('grid_fees')
    def grid_fees(self, fee_cents_per_kWh):
        if self._post_request('grid_fees', {"fee": fee_cents_per_kWh}):
            return self.dispatcher.wait_for_command_response('grid_fees')
