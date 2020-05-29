import logging

from d3a_api_client.utils import logging_decorator
from d3a_api_client.websocket_device import WebsocketMessageReceiver
from d3a_api_client.rest_device import RestDeviceClient

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
            else:
                logging.error(f"Received message with unknown event type: {message}")
        elif "command" in message:
            self.command_response_buffer.append(message)


class Aggregator(RestDeviceClient):

    def __init__(self, simulation_id, domain_name, aggregator_name,
                 websockets_domain_name, autoregister=False, accept_all_devices=True):
        super().__init__(simulation_id, domain_name, aggregator_name,
                 websockets_domain_name, autoregister)

        self.aggregator_name = aggregator_name
        self.accept_all_devices = accept_all_devices
        self.device_uuid_list = []

    @logging_decorator('register')
    def register(self, is_blocking=True):
        aggregator_dict = {"aggregator_name": self.aggregator_name}
        transaction_id, posted = self._post_request('register', aggregator_dict)
        if posted:
            return_value = self.dispatcher.wait_for_command_response('register', transaction_id)
            if "uuid" in return_value:
                self.device_id = return_value["uuid"]
            else:
                raise Exception(f"d3a did register the aggregator {self.aggregator_name}")
            self.registered = return_value["registered"]
            return return_value

    @logging_decorator('unregister')
    def unregister(self, is_blocking):
        aggregator_dict = {"aggregator_name": self.aggregator_name, "uuid": self.device_id}
        transaction_id, posted = self._post_request('unregister', aggregator_dict)
        if posted:
            return_value = self.dispatcher.wait_for_command_response('unregister', transaction_id)
            self.registered = False
            return return_value

    def _selected_by_device(self, message):
        device_dict = {"uuid": message["device_uuid"], "type":  message["device_type"]}
        if self.accept_all_devices:
            self.device_uuid_list.append(device_dict)

    def unselect_device(self, device_uuid):
        if device_uuid in self.device_uuid_list:
            self.device_uuid_list.remove(device_uuid)

    def _all_uuids_in_selected_device_uuid_list(self, uuid_list):
        for device_uuid in uuid_list:
            if device_uuid not in self.device_uuid_list:
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
        transaction_id, posted = self._post_request('batch_command', batch_command_dict)
        if posted:
            return self.dispatcher.wait_for_command_response('batch_command', transaction_id)
