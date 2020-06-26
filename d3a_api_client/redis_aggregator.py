import logging
import uuid
import json
from d3a_api_client.utils import logging_decorator, blocking_get_request, \
    blocking_post_request
from d3a_api_client.websocket_device import WebsocketMessageReceiver, WebsocketThread
from concurrent.futures.thread import ThreadPoolExecutor
from d3a_api_client.rest_device import RestDeviceClient
from d3a_api_client.constants import MAX_WORKER_THREADS
from d3a_api_client.redis_device import RedisDeviceClient
from redis import StrictRedis
from d3a_interface.utils import wait_until_timeout_blocking

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)


class RedisAPIException(Exception):
    pass


class RedisAggregator:

    def __init__(self, aggregator_name, autoregister=False,
                 accept_all_devices=True, redis_url='redis://localhost:6379'):

        self.redis_db = StrictRedis.from_url(redis_url)
        self.pubsub = self.redis_db.pubsub()
        self.aggregator_name = aggregator_name
        self.autoregister = autoregister
        self.accept_all_devices = accept_all_devices
        self._transaction_id_buffer = []
        self.device_uuid_list = []
        self.aggregator_uuid = None
        self._subscribe_to_response_channels()
        self._connect_to_simulation(is_blocking=False)

    def _connect_to_simulation(self, is_blocking=False):
        if self.aggregator_uuid is None:
            aggr_id = self._create_aggregator(is_blocking=is_blocking)
            self.aggregator_uuid = aggr_id

    def _subscribe_to_response_channels(self):

        channel_dict = {"crud_aggregator_response": self._aggregator_response_callback}

        self.pubsub.subscribe(**channel_dict)
        self.pubsub.run_in_thread(daemon=True)

    def _aggregator_response_callback(self, message):
        logging.info(f"message: {message}")
        data = json.loads(message['data'])
        logging.info(f"data: {data}")
        logging.info(f"transaction_id: {data['transaction_id']}")

        if data['transaction_id'] in self._transaction_id_buffer:
            self._transaction_id_buffer.pop(self._transaction_id_buffer.index(data['transaction_id']))

    def _check_transaction_id_cached_out(self, transaction_id):
        return transaction_id in self._transaction_id_buffer

    def _create_aggregator(self, is_blocking=True):
        logging.info(f"Trying to create aggregator {self.aggregator_name}")

        transaction_id = str(uuid.uuid4())
        data = {"name": self.aggregator_name, "type": "CREATE", "transaction_id": transaction_id}
        self.redis_db.publish(f'crud_aggregator', json.dumps(data))
        self._transaction_id_buffer.append(transaction_id)

        if is_blocking:
            try:
                wait_until_timeout_blocking(
                    lambda: self._check_transaction_id_cached_out(transaction_id)
                )
                return transaction_id
            except AssertionError:
                raise RedisAPIException(f'API registration process timed out.')

    def delete_aggregator(self, is_blocking=True):
        logging.info(f"Trying to create aggregator {self.aggregator_name}")

        transaction_id = str(uuid.uuid4())
        data = {"name": self.aggregator_name,
                "type": "DELETE",
                "transaction_id": self.aggregator_uuid}
        self.redis_db.publish(f'crud_aggregator', json.dumps(data))
        self._transaction_id_buffer.append(transaction_id)

        if is_blocking:
            try:
                wait_until_timeout_blocking(
                    lambda: self._check_transaction_id_cached_out(transaction_id)
                )
                return transaction_id
            except AssertionError:
                raise RedisAPIException(f'API has timed out.')

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
