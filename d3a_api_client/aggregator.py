import logging
import traceback
from d3a_api_client.commands import ClientCommandBuffer
from d3a_api_client.utils import logging_decorator, blocking_get_request, \
    blocking_post_request
from d3a_api_client.websocket_device import WebsocketMessageReceiver, WebsocketThread
from concurrent.futures.thread import ThreadPoolExecutor
from d3a_api_client.rest_device import RestDeviceClient
from d3a_api_client.constants import MAX_WORKER_THREADS
from d3a_api_client.grid_fee_calculation import GridFeeCalculation


class AggregatorWebsocketMessageReceiver(WebsocketMessageReceiver):
    def __init__(self, rest_client):
        super().__init__(rest_client)

    def _handle_event_message(self, message):
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


class Aggregator(RestDeviceClient):

    def __init__(self, simulation_id, domain_name, aggregator_name,
                 websockets_domain_name, accept_all_devices=True):
        super().__init__(
            simulation_id=simulation_id, device_id="", domain_name=domain_name,
            websockets_domain_name=websockets_domain_name, autoregister=False,
            start_websocket=False)

        self.grid_fee_calculation = GridFeeCalculation()
        self.aggregator_name = aggregator_name
        self.accept_all_devices = accept_all_devices
        self.device_uuid_list = []
        self.aggregator_uuid = None
        self._client_command_buffer = ClientCommandBuffer()
        self._connect_to_simulation()

    def _connect_to_simulation(self):
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
                                                self.websockets_domain_name, self.domain_name,
                                                self.dispatcher)
        self.websocket_thread.start()
        self.callback_thread = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)

    @logging_decorator('create_aggregator')
    def list_aggregators(self):
        list_of_aggregators = blocking_get_request(f'{self.aggregator_prefix}list-aggregators/', {}, self.jwt_token)
        if list_of_aggregators is None:
            logging.error(f"No aggregators found on {self.aggregator_prefix}")
            list_of_aggregators = []
        return list_of_aggregators

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

    @property
    def add_to_batch_commands(self):
        """
        A property which is meant to be accessed prefixed to a chained function from the ClientCommandBuffer class
        This command will be added to the batch commands buffer
        """
        return self._client_command_buffer

    def batch_command(self, batch_command_dict):
        # TODO: Remove this as this is kept atm for the backward compatibility
        self._all_uuids_in_selected_device_uuid_list(batch_command_dict.keys())
        transaction_id, posted = self._post_request(
            'batch-commands', {"aggregator_uuid": self.aggregator_uuid, "batch_commands": batch_command_dict})
        if posted:
            return self.dispatcher.wait_for_command_response('batch_commands', transaction_id)

    @property
    def commands_buffer_length(self):
        """
        Returns the length of the batch commands buffer
        """
        return self._client_command_buffer.buffer_length

    def execute_batch_commands(self):
        if not self.commands_buffer_length:
            return
        batch_command_dict = self._client_command_buffer.execute_batch()
        self._all_uuids_in_selected_device_uuid_list(batch_command_dict.keys())
        transaction_id, posted = self._post_request(
            'batch-commands', {"aggregator_uuid": self.aggregator_uuid, "batch_commands": batch_command_dict})
        if posted:
            self._client_command_buffer.clear()
            return self.dispatcher.wait_for_command_response('batch_commands', transaction_id)

    def _on_market_cycle(self, message):
        self.grid_fee_calculation.handle_grid_stats(message)
        super()._on_market_cycle(message)

    def calculate_grid_fee(self, start_market_or_device_name: str,
                           target_market_or_device_name: str = None,
                           fee_type: str = "next_market_fee"):
        return self.grid_fee_calculation.calculate_grid_fee(start_market_or_device_name,
                                                            target_market_or_device_name, fee_type)