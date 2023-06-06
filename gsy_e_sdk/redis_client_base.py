import json
import logging
import uuid
from concurrent.futures.thread import ThreadPoolExecutor

from gsy_framework.redis_channels import ExternalStrategyChannels, AggregatorChannels
from gsy_framework.utils import (
    execute_function_util, wait_until_timeout_blocking, key_in_dict_and_not_none)
from redis import Redis

from gsy_e_sdk import APIClientInterface
from gsy_e_sdk.constants import MAX_WORKER_THREADS, LOCAL_REDIS_URL


class RedisAPIException(Exception):
    """Exception for issues with redis"""


class RedisClientBase(APIClientInterface):
    # pylint: disable=too-many-instance-attributes
    """Base class for redis client"""

    def __init__(self, area_id, autoregister=True, redis_url=LOCAL_REDIS_URL,
                 pubsub_thread=None):
        super().__init__(area_id, autoregister, redis_url)
        self.area_uuid = None
        self.channel_names = ExternalStrategyChannels(False, "", asset_name=area_id)
        self.redis_db = Redis.from_url(redis_url)
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
            self.channel_names.register_response: self._on_register,
            self.channel_names.unregister_response: self._on_unregister,
            f"{self.area_id}/*": self._on_event_or_response}

        b_aggregator_response = AggregatorChannels.response().encode("utf-8")
        if b_aggregator_response in self.pubsub.patterns:
            self._subscribed_aggregator_response_cb = self.pubsub.patterns[b_aggregator_response]
        channel_subs[AggregatorChannels.response()] = self._aggregator_response_callback

        self.pubsub.psubscribe(**channel_subs)
        if pubsub_thread is None:
            self.pubsub.run_in_thread(daemon=True)

    def _aggregator_response_callback(self, message):
        if self._subscribed_aggregator_response_cb is not None:
            self._subscribed_aggregator_response_cb(message)
        data = json.loads(message["data"])
        if not self._is_transaction_response_received(data["transaction_id"]):
            self._transaction_id_buffer.pop(
                self._transaction_id_buffer.index(data["transaction_id"])
            )

    def _check_buffer_message_matching_command_and_id(self, message):
        if key_in_dict_and_not_none(message, "transaction_id"):
            transaction_id = message["transaction_id"]
            if not any(command in ["register", "unregister"] and "transaction_id" in data and
                       data["transaction_id"] == transaction_id
                       for command, data in self._blocking_command_responses.items()):
                raise RedisAPIException(
                    "There is no matching command response in _blocking_command_responses."
                )
        else:
            raise RedisAPIException(
                "The answer message does not contain a valid 'transaction_id' member.")

    def _is_transaction_response_received(self, transaction_id):
        return transaction_id not in self._transaction_id_buffer

    def register(self, is_blocking=True):
        logging.info("Trying to register to %s", self.area_id)
        if self.is_active:
            raise RedisAPIException("API is already registered to the market.")
        data = {"name": self.area_id, "transaction_id": str(uuid.uuid4())}
        self._blocking_command_responses["register"] = data
        self.redis_db.publish(self.channel_names.register, json.dumps(data))

        if is_blocking:
            try:
                wait_until_timeout_blocking(lambda: self.is_active, timeout=120)
            except AssertionError as ex:
                raise RedisAPIException(
                    "API registration process timed out. Server will continue processing your "
                    "request on the background and will notify you as soon as the registration "
                    "has been completed.") from ex

    def unregister(self, is_blocking=True):
        logging.info("Trying to unregister from %s", self.area_id)

        if not self.is_active:
            raise RedisAPIException("API is already unregistered from the market.")

        data = {"name": self.area_id, "transaction_id": str(uuid.uuid4())}
        self._blocking_command_responses["unregister"] = data
        self.redis_db.publish(self.channel_names.unregister, json.dumps(data))

        if is_blocking:
            try:
                wait_until_timeout_blocking(lambda: not self.is_active, timeout=120)
            except AssertionError as ex:
                raise RedisAPIException(
                    "API unregister process timed out. Server will continue processing your "
                    "request on the background and will notify you as soon as the unregistration "
                    "has been completed.") from ex

    def _on_register(self, msg):
        message = json.loads(msg["data"])
        self._check_buffer_message_matching_command_and_id(message)
        self.area_uuid = message["device_uuid"]

        logging.info("%s was registered", self.area_id)
        self.is_active = True

        def executor_function():
            self.on_register(message)

        self.executor.submit(executor_function)

    def _on_unregister(self, msg):
        message = json.loads(msg["data"])
        self._check_buffer_message_matching_command_and_id(message)
        if message.get("response") != "success":
            raise RedisAPIException(
                f"Failed to unregister from market {self.area_id}. Deactivating connection.")

        self.is_active = False

    def _on_event_or_response(self, msg):
        message = json.loads(msg["data"])
        self.executor.submit(execute_function_util,
                             function=lambda: self.on_event_or_response(message),
                             function_name="on_event_or_response")

    def select_aggregator(self, aggregator_uuid, is_blocking=True):
        """Send select aggregator command to gsy-e."""
        if not self.area_uuid:
            raise RedisAPIException("The device/market has not ben registered yet, "
                                    "can not select an aggregator")
        logging.info("%s is trying to select aggregator %s", self.area_id, aggregator_uuid)

        transaction_id = str(uuid.uuid4())
        data = {"aggregator_uuid": aggregator_uuid,
                "device_uuid": self.area_uuid,
                "type": "SELECT",
                "transaction_id": transaction_id}
        self._transaction_id_buffer.append(transaction_id)
        self.redis_db.publish(AggregatorChannels.commands, json.dumps(data))

        if is_blocking:
            try:
                wait_until_timeout_blocking(
                    lambda: self._is_transaction_response_received(transaction_id)
                )
                logging.info("%s has selected AGGREGATOR: %s", self.area_id, aggregator_uuid)
                return transaction_id
            except AssertionError as ex:
                raise RedisAPIException("API has timed out.") from ex
        return None

    def unselect_aggregator(self, aggregator_uuid):
        """Send unselect aggregator command to gsy-e."""
        raise NotImplementedError("unselect_aggregator hasn't been implemented yet.")

    def on_register(self, registration_info):
        """Callback for register response"""

    def on_event_or_response(self, message):
        pass
