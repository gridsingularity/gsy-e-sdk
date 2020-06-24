import logging

from d3a_api_client.utils import logging_decorator, blocking_get_request, \
    blocking_post_request
from d3a_api_client.websocket_device import WebsocketMessageReceiver, WebsocketThread
from concurrent.futures.thread import ThreadPoolExecutor
from d3a_api_client.rest_device import RestDeviceClient
from d3a_api_client.constants import MAX_WORKER_THREADS


root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)


class AggregatorWebsocketMessageReceiver(WebsocketMessageReceiver):
    def __init__(self, rest_client):
        super().__init__(rest_client)

    def received_message(self, message):
        if "event" in message:
            if message["event"] == "market":
                self.client._on_market_cycle(message)
            elif message["event"] == "tick":
                self.client._on_tick(message)
            elif message["event"] == "trade":
                self.client._on_trade(message)
            elif message["event"] == "finish":
                self.client._on_finish(message)
            elif message["event"] == "selected_by_device":
                self.client._selected_by_device(message)
            elif message["event"] == "unselected_by_device":
                self.client._unselected_by_device(message)
            else:
                logging.error(f"Received message with unknown event type: {message}")
        elif "command" in message:
            self.command_response_buffer.append(message)


class Aggregator(RestDeviceClient):

    def __init__(self, simulation_id, domain_name, aggregator_name,
                 websockets_domain_name, accept_all_devices=True):
        super().__init__(
            simulation_id=simulation_id, device_id="", domain_name=domain_name,
            websockets_domain_name=websockets_domain_name, autoregister=False, start_websocket=False)

        self.aggregator_name = aggregator_name
        self.accept_all_devices = accept_all_devices
        self.device_uuid_list = []
        self.aggregator_uuid = None
        user_aggrs = self.list_aggregators()
        for a in user_aggrs:
            if a["name"] == self.aggregator_name:
                self.aggregator_uuid = a["uuid"]
        if self.aggregator_uuid is None:
            aggr = self._create_aggregator()
            self.aggregator_uuid = aggr["uuid"]
        self.start_websocket_connection()

    def start_websocket_connection(self):
        self.dispatcher = AggregatorWebsocketMessageReceiver(self)
        self.websocket_thread = WebsocketThread(self.simulation_id, f"aggregator/{self.aggregator_uuid}",
                                                self.jwt_token,
                                                self.websockets_domain_name, self.dispatcher)
        self.websocket_thread.start()
        self.callback_thread = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)

    @logging_decorator('create_aggregator')
    def list_aggregators(self):
        return blocking_get_request(f'{self.aggregator_prefix}list-aggregators/', {}, self.jwt_token)

    @property
    def _url_prefix(self):
        return f'{self.domain_name}/external-connection/aggregator-api/{self.simulation_id}'

    @logging_decorator('create_aggregator')
    def _create_aggregator(self):
        return blocking_post_request(f'{self.aggregator_prefix}create-aggregator/',
                                     {"name": self.aggregator_name}, self.jwt_token)

    @logging_decorator('create_aggregator')
    def delete_aggregator(self):
        return blocking_post_request(f'{self.aggregator_prefix}delete-aggregator/',
                                     {"aggregator_uuid": self.aggregator_uuid}, self.jwt_token)

    def _selected_by_device(self, message):
        if self.accept_all_devices:
            self.device_uuid_list.append(message["device_uuid"])

    def _unselected_by_device(self, message):
        device_uuid = message["device_uuid"]
        if device_uuid in self.device_uuid_list:
            self.device_uuid_list.remove(device_uuid)

    def _all_uuids_in_selected_device_uuid_list(self, uuid_list):
        for device_uuid in uuid_list:
            if device_uuid not in self.device_uuid_list:
                logging.error(f"{device_uuid} not in list of selected device uuids {self.device_uuid_list}")
                raise Exception(f"{device_uuid} not in list of selected device uuids")
        return True

    def batch_command(self, batch_command_dict):
        """
        batch_dict : dict where keys are device_uuids and values list of commands
        e.g.: batch_dict = {
                        "dev_uuid1": [{"energy": 10, "rate": 30, "type": "offer"}, {"energy": 9, "rate": 12, "type": "bid"}],
                        "dev_uuid2": [{"energy": 20, "rate": 60, "type": "bid"}, {"type": "list_market_stats"}]
                        }
        """
        self._all_uuids_in_selected_device_uuid_list(batch_command_dict.keys())
        transaction_id, posted = self._post_request(
            'batch-commands', {"aggregator_uuid": self.aggregator_uuid, "batch_commands": batch_command_dict})
        if posted:
            return self.dispatcher.wait_for_command_response('batch_commands', transaction_id)
