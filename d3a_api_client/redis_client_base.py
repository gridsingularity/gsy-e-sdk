import json
import logging
import uuid
from concurrent.futures.thread import ThreadPoolExecutor

from d3a_interface.utils import wait_until_timeout_blocking, key_in_dict_and_not_none
from redis import StrictRedis

from d3a_api_client import APIClientInterface
from d3a_api_client.constants import MAX_WORKER_THREADS
from d3a_api_client.utils import execute_function_util


class RedisAPIException(Exception):
    pass


class RedisClientBase(APIClientInterface):
    def __init__(self, area_id, autoregister=True, redis_url='redis://localhost:6379',
                 pubsub_thread=None):
        super().__init__(area_id, autoregister, redis_url)
        self.area_uuid = None
        self.redis_db = StrictRedis.from_url(redis_url)
        self.pubsub = self.redis_db.pubsub() if pubsub_thread is None else pubsub_thread
        self.area_id = area_id
        self.device_uuid = None
        self.is_active = False
        self._blocking_command_responses = {}
        self._transaction_id_buffer = []
        self._subscribed_aggregator_response_cb = None
        self._subscribe_to_response_channels(pubsub_thread)
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)
        if autoregister:
            self.register(is_blocking=True)

    def _subscribe_to_response_channels(self, pubsub_thread=None):
        channel_subs = {
            f"{self.area_id}/response/register_participant": self._on_register,
            f"{self.area_id}/response/unregister_participant": self._on_unregister,
            f"{self.area_id}/*": self._on_event_or_response}

        if b'aggregator_response' in self.pubsub.patterns:
            self._subscribed_aggregator_response_cb = self.pubsub.patterns[b'aggregator_response']
        channel_subs["aggregator_response"] = self._aggregator_response_callback

        self.pubsub.psubscribe(**channel_subs)
        if pubsub_thread is None:
            self.pubsub.run_in_thread(daemon=True)

    def _aggregator_response_callback(self, message):
        if self._subscribed_aggregator_response_cb is not None:
            self._subscribed_aggregator_response_cb(message)
        data = json.loads(message['data'])
        if data['transaction_id'] in self._transaction_id_buffer:
            self._transaction_id_buffer.pop(self._transaction_id_buffer.index(data['transaction_id']))

    def _check_buffer_message_matching_command_and_id(self, message):
        if key_in_dict_and_not_none(message, "transaction_id"):
            transaction_id = message["transaction_id"]
            if not any(command in ["register", "unregister"] and "transaction_id" in data and
                       data["transaction_id"] == transaction_id
                       for command, data in self._blocking_command_responses.items()):
                raise RedisAPIException("There is no matching command response in _blocking_command_responses.")
        else:
            raise RedisAPIException(
                "The answer message does not contain a valid 'transaction_id' member.")

    def _check_transaction_id_cached_out(self, transaction_id):
        return transaction_id not in self._transaction_id_buffer

    def register(self, is_blocking=True):
        logging.info(f"Trying to register to {self.area_id}")
        if self.is_active:
            raise RedisAPIException(f'API is already registered to the market.')
        data = {"name": self.area_id, "transaction_id": str(uuid.uuid4())}
        self._blocking_command_responses["register"] = data
        self.redis_db.publish(f'{self.area_id}/register_participant', json.dumps(data))

        if is_blocking:
            try:
                wait_until_timeout_blocking(lambda: self.is_active, timeout=120)
            except AssertionError:
                raise RedisAPIException(
                    f'API registration process timed out. Server will continue processing your '
                    f'request on the background and will notify you as soon as the registration '
                    f'has been completed.')

    def unregister(self, is_blocking=True):
        logging.info(f"Trying to unregister from {self.area_id}")

        if not self.is_active:
            raise RedisAPIException(f'API is already unregistered from the market.')

        data = {"name": self.area_id, "transaction_id": str(uuid.uuid4())}
        self._blocking_command_responses["unregister"] = data
        self.redis_db.publish(f'{self.area_id}/unregister_participant', json.dumps(data))

        if is_blocking:
            try:
                wait_until_timeout_blocking(lambda: not self.is_active, timeout=120)
            except AssertionError:
                raise RedisAPIException(
                    f'API unregister process timed out. Server will continue processing your '
                    f'request on the background and will notify you as soon as the unregistration '
                    f'has been completed.')

    def _on_register(self, msg):
        message = json.loads(msg["data"])
        self._check_buffer_message_matching_command_and_id(message)
        self.area_uuid = message["device_uuid"]

        logging.info(f"{self.area_id} was registered")
        self.is_active = True

        def executor_function():
            self.on_register(message)

        self.executor.submit(executor_function)

    def _on_unregister(self, msg):
        message = json.loads(msg["data"])
        self._check_buffer_message_matching_command_and_id(message)
        self.is_active = False
        if message["response"] != "success":
            raise RedisAPIException(f'Failed to unregister from market {self.area_id}.'
                                    f'Deactivating connection.')

    def _on_event_or_response(self, msg):
        message = json.loads(msg["data"])
        function = lambda: self.on_event_or_response(message)
        self.executor.submit(execute_function_util, function=function,
                             function_name="on_event_or_response")

    def select_aggregator(self, aggregator_uuid, is_blocking=True):
        if not self.area_uuid:
            raise RedisAPIException("The device/market has not ben registered yet, "
                                    "can not select an aggregator")
        logging.info(f"{self.area_id} is trying to select aggregator {aggregator_uuid}")

        transaction_id = str(uuid.uuid4())
        data = {"aggregator_uuid": aggregator_uuid,
                "device_uuid": self.area_uuid,
                "type": "SELECT",
                "transaction_id": transaction_id}
        self._transaction_id_buffer.append(transaction_id)
        self.redis_db.publish("aggregator", json.dumps(data))

        if is_blocking:
            try:
                wait_until_timeout_blocking(
                    lambda: self._check_transaction_id_cached_out(transaction_id)
                )
                logging.info(f"{self.area_id} has selected AGGREGATOR: {aggregator_uuid}")
                return transaction_id
            except AssertionError:
                raise RedisAPIException(f'API has timed out.')

    def unselect_aggregator(self, aggregator_uuid):
        raise NotImplementedError("unselect_aggregator hasn't been implemented yet.")

    def on_register(self, registration_info):
        pass

    def on_event_or_response(self, message):
        pass
