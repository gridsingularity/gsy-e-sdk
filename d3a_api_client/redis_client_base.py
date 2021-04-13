import json
import logging
import uuid
from concurrent.futures.thread import ThreadPoolExecutor
from functools import wraps

from d3a_interface.utils import wait_until_timeout_blocking, key_in_dict_and_not_none
from redis import StrictRedis

from d3a_api_client import APIClientInterface
from d3a_api_client.constants import MAX_WORKER_THREADS


class RedisAPIException(Exception):
    pass


def registered_connection(f):
    @wraps(f)
    def wrapped(self, *args, **kwargs):
        if not self.is_active:
            raise RedisAPIException(f'Registration has not completed yet.')
        return f(self, *args, **kwargs)

    return wrapped


class RedisClient(APIClientInterface):
    def __init__(self, area_id, client_id, autoregister=True, redis_url='redis://localhost:6379',
                 pubsub_thread=None):
        super().__init__(area_id, client_id, autoregister, redis_url)
        self.redis_db = StrictRedis.from_url(redis_url)
        self.pubsub = self.redis_db.pubsub() if pubsub_thread is None else pubsub_thread
        self.area_id = area_id
        self.client_id = client_id
        self.device_uuid = None
        self.is_active = False
        self._blocking_command_responses = {}
        self._subscribe_to_response_channels(pubsub_thread)
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)
        if autoregister:
            self.register(is_blocking=True)

    def _subscribe_to_response_channels(self, pubsub_thread=None):
        channel_subs = {
            f"{self.area_id}/response/register_participant": self._on_register,
            f"{self.area_id}/response/unregister_participant": self._on_unregister,
        }

        if b'aggregator_response' in self.pubsub.patterns:
            self._subscribed_aggregator_response_cb = self.pubsub.patterns[b'aggregator_response']

        self.pubsub.psubscribe(**channel_subs)
        if pubsub_thread is None:
            self.pubsub.run_in_thread(daemon=True)

    def register(self, is_blocking=False):
        logging.info(f"Trying to register to {self.area_id} as client {self.client_id}")
        if self.is_active:
            raise RedisAPIException(f'API is already registered to the market.')
        data = {"name": self.client_id, "transaction_id": str(uuid.uuid4())}
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

    def unregister(self, is_blocking=False):
        logging.info(f"Trying to unregister from {self.area_id} as client {self.client_id}")

        if not self.is_active:
            raise RedisAPIException(f'API is already unregistered from the market.')

        data = {"name": self.client_id, "transaction_id": str(uuid.uuid4())}
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

    @property
    def _channel_prefix(self):
        return f"{self.area_id}/{self.client_id}"

    def _wait_and_consume_command_response(self, command_type, transaction_id):
        def check_if_command_response_received():
            return any(command == command_type and
                       "transaction_id" in data and data["transaction_id"] == transaction_id
                       for command, data in self._blocking_command_responses.items())

        logging.debug(f"Command {command_type} waiting for response...")
        wait_until_timeout_blocking(check_if_command_response_received, timeout=120)
        command_output = self._blocking_command_responses.pop(command_type)
        logging.debug(f"Command {command_type} got response {command_output}")
        return command_output

    def _generate_command_response_callback(self, command_type):
        def _command_received(msg):
            try:
                message = json.loads(msg["data"])
            except Exception as e:
                logging.error(f"Received incorrect response on command {command_type}. "
                              f"Response {msg}. Error {e}.")
                return
            logging.debug(f"Command {command_type} received response: {message}")
            if 'error' in message:
                logging.error(f"Error when receiving {command_type} command response."
                              f"Error output: {message}")
                return
            else:
                self._blocking_command_responses[command_type] = message

        return _command_received

    def _on_register(self, msg):
        message = json.loads(msg["data"])
        self._check_buffer_message_matching_command_and_id(message)
        if 'available_publish_channels' not in message or \
                'available_subscribe_channels' not in message:
            raise RedisAPIException(f'Registration to the market {self.area_id} failed.')

        logging.info(f"Client was registered to market: {message}")
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

