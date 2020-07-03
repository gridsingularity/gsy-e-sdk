import logging
import uuid
import json
from redis import StrictRedis
from d3a_interface.utils import wait_until_timeout_blocking
from concurrent.futures.thread import ThreadPoolExecutor
from d3a_api_client.constants import MAX_WORKER_THREADS
from threading import Lock
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
        self.aggregator_uuid = None
        self.autoregister = autoregister
        self.accept_all_devices = accept_all_devices
        self._transaction_id_buffer = []
        self._transaction_id_response_buffer = {}
        self.device_uuid_list = []
        self._subscribe_to_response_channels()
        self._connect_to_simulation(is_blocking=True)
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)
        self.lock = Lock()

    def _connect_to_simulation(self, is_blocking=True):
        if self.aggregator_uuid is None:
            aggr_id = self._create_aggregator(is_blocking=is_blocking)
            self.aggregator_uuid = aggr_id

    def _subscribe_to_response_channels(self):
        event_channel = f'external-aggregator/*/*/events/all'
        channel_dict = {"crud_aggregator_response": self._aggregator_response_callback,
                        event_channel: self._events_callback_dict,
                        f"external-aggregator//*/response/batch_commands":
                            self._batch_response,
        }

        self.pubsub.psubscribe(**channel_dict)
        self.pubsub.run_in_thread(daemon=True)

    def _batch_response(self, message):
        data = json.loads(message['data'])
        if self.aggregator_uuid != data['aggregator_uuid']:
            return
        logging.info(f"Market Stats. Information: {message}")
        with self.lock:
            self._transaction_id_buffer.pop(self._transaction_id_buffer.index(data['transaction_id']))
            self._transaction_id_response_buffer[data['transaction_id']] = data['responses']

        def executor_function():
            self.on_batch_response(data['responses'])
        self.executor.submit(executor_function)

    def _aggregator_response_callback(self, message):
        data = json.loads(message['data'])

        if data['transaction_id'] in self._transaction_id_buffer:
            self._transaction_id_buffer.pop(self._transaction_id_buffer.index(data['transaction_id']))
        if data['status'] == "SELECTED":
            self._selected_by_device(data)

    def _events_callback_dict(self, message):
        payload = json.loads(message['data'])
        if "event" in payload and payload['event'] == 'market':
            self._on_market_cycle(payload)
        elif "event" in payload and payload["event"] == "tick":
            self._on_tick(payload)
        elif "event" in payload and payload["event"] == "trade":
            self._on_trade(payload)
        elif "event" in payload and payload["event"] == "finish":
            self._on_finish(payload)

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
        logging.info(f"Trying to delete aggregator {self.aggregator_name}")

        transaction_id = str(uuid.uuid4())
        data = {"name": self.aggregator_name,
                "aggregator_uuid": self.aggregator_uuid,
                "type": "DELETE",
                "transaction_id": transaction_id}
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

    def batch_command(self, batch_command_dict, is_blocking=True):
        """
        batch_dict : dict where keys are device_uuids and values list of commands
        e.g.: batch_dict = {
                        "dev_uuid1": [{"energy": 10, "rate": 30, "type": "offer"}, {"energy": 9, "rate": 12, "type": "bid"}],
                        "dev_uuid2": [{"energy": 20, "rate": 60, "type": "bid"}, {"type": "list_market_stats"}]
                        }
        """
        self._all_uuids_in_selected_device_uuid_list(batch_command_dict.keys())
        transaction_id = str(uuid.uuid4())
        batched_command = {"type": "BATCHED", "transaction_id": transaction_id,
                           "aggregator_uuid": self.aggregator_uuid,
                           "batch_commands": batch_command_dict}
        batch_channel = f'external//aggregator/{self.aggregator_uuid}/batch_commands'
        self.redis_db.publish(batch_channel, json.dumps(batched_command))
        self._transaction_id_buffer.append(transaction_id)

        if is_blocking:
            try:
                wait_until_timeout_blocking(
                    lambda: not self._check_transaction_id_cached_out(transaction_id)
                )
                return self._transaction_id_response_buffer.get(transaction_id, None)
            except AssertionError:
                raise RedisAPIException(f'API registration process timed out.')

    def _on_market_cycle(self, message):
        logging.info(f"A new market was created. Market information: {message}")

        def executor_function():
            self.on_market_cycle(message)
        self.executor.submit(executor_function)

    def _on_tick(self, message):
        logging.info(f"Time has elapsed on the device. Progress info: {message}")

        def executor_function():
            self.on_tick(message)
        self.executor.submit(executor_function)

    def _on_trade(self, message):
        logging.info(f"A trade took place on the device. Trade information: {message}")

        def executor_function():
            self.on_trade(message)
        self.executor.submit(executor_function)

    def _on_finish(self, message):
        logging.info(f"Simulation finished. Information: {message}")

        def executor_function():
            self.on_finish(message)
        self.executor.submit(executor_function)

    def on_market_cycle(self, market_info):
        pass

    def on_tick(self, tick_info):
        pass

    def on_trade(self, trade_info):
        pass

    def on_finish(self, finish_info):
        pass

    def on_batch_response(self, message):
        pass
