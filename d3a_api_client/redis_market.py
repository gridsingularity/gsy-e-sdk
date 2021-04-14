import logging
import json
import uuid
from slugify import slugify
from redis import StrictRedis
from concurrent.futures.thread import ThreadPoolExecutor

from d3a_interface.utils import wait_until_timeout_blocking, key_in_dict_and_not_none
from d3a_api_client.constants import MAX_WORKER_THREADS
from d3a_api_client.utils import RedisAPIException, execute_function_util, log_market_progression


class RedisMarketClient:
    def __init__(self, area_id, redis_url='redis://localhost:6379', autoregister=True):
        self.area_slug = slugify(area_id, to_lower=True)
        self.area_uuid = None
        self.redis_db = StrictRedis.from_url(redis_url)
        self.pubsub = self.redis_db.pubsub()
        self._subscribe_to_response_channels()
        self._blocking_command_responses = {}
        self._transaction_id_buffer = []
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)
        self.is_active = False
        if autoregister:
            self.register(is_blocking=True)

    def register(self, is_blocking=False):
        logging.info(f"Trying to register {self.area_slug}")
        if self.is_active:
            raise RedisAPIException(f'API is already registered to the market.')
        data = {"name": self.area_slug, "transaction_id": str(uuid.uuid4())}
        self._blocking_command_responses["register"] = data
        self.redis_db.publish(f'{self._channel_prefix}/register_participant', json.dumps(data))

        if is_blocking:
            try:
                wait_until_timeout_blocking(lambda: self.is_active, timeout=120)
            except AssertionError:
                raise RedisAPIException(
                    f'API registration process timed out. Server will continue processing your '
                    f'request on the background and will notify you as soon as the registration '
                    f'has been completed.')

    def unregister(self, is_blocking=False):
        logging.info(f"Trying to unregister {self.area_slug}")

        if not self.is_active:
            raise RedisAPIException(f'API is already unregistered from the market.')

        data = {"name": self.area_slug, "transaction_id": str(uuid.uuid4())}
        self._blocking_command_responses["unregister"] = data
        self.redis_db.publish(f'{self._channel_prefix}/unregister_participant', json.dumps(data))

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
        return f"{self.area_slug}"

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

    def _subscribe_to_response_channels(self):
        channel_subs = {
            f"{self._channel_prefix}/response/{command_name}":
                self._generate_command_response_callback(command_name)
            for command_name in ["grid_fees", "dso_market_stats"]}
        channel_subs.update({
            f'{self._channel_prefix}/response/register_participant': self._on_register,
            f'{self._channel_prefix}/response/unregister_participant': self._on_unregister,
            f'{self._channel_prefix}/market-events/market': self._on_market_cycle,
            f'{self._channel_prefix}/events/finish': self._on_finish,
            f'{self._channel_prefix}/*': self._on_event_or_response
        })

        self.pubsub.psubscribe(**channel_subs)
        self.pubsub.run_in_thread(daemon=True)

    def _check_transaction_id_cached_out(self, transaction_id):
        return transaction_id in self._transaction_id_buffer

    def _on_register(self, msg):
        message = json.loads(msg["data"])
        self._check_buffer_message_matching_command_and_id(message)
        self.area_uuid = message["device_uuid"]
        self.is_active = True

    def _on_unregister(self, msg):
        message = json.loads(msg["data"])
        self._check_buffer_message_matching_command_and_id(message)
        self.is_active = False

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

    def select_aggregator(self, aggregator_uuid, is_blocking=True, unsubscribe_from_device_events=True):
        if not self.area_uuid:
            assert False
        logging.info(f"Market: {self.area_slug} is trying to select aggregator {aggregator_uuid}")

        transaction_id = str(uuid.uuid4())
        data = {"aggregator_uuid": aggregator_uuid,
                "device_uuid": self.area_uuid,
                "type": "SELECT",
                "transaction_id": transaction_id}
        self.redis_db.publish(f'aggregator', json.dumps(data))
        self._transaction_id_buffer.append(transaction_id)

        if is_blocking:
            try:
                wait_until_timeout_blocking(
                    lambda: self._check_transaction_id_cached_out(transaction_id)
                )
                logging.info(f"MARKET: {self.area_slug} has selected "
                                f"AGGREGATOR: {aggregator_uuid}")
                return transaction_id
            except AssertionError:
                raise RedisAPIException(f'API has timed out.')

        if unsubscribe_from_device_events:
            self.pubsub.unsubscribe(
                [f'{self._channel_prefix}/response/register_participant',
                 f'{self._channel_prefix}/response/unregister_participant',
                 f'{self._channel_prefix}/market-events/market',
                 f'{self._channel_prefix}/events/finish']
            )

    def unselect_aggregator(self, aggregator_uuid):
        raise NotImplementedError("unselect_aggregator hasn't been implemented yet.")

    def _wait_and_consume_command_response(self, command_type):
        logging.debug(f"Command {command_type} waiting for response...")
        wait_until_timeout_blocking(lambda: command_type in self._blocking_command_responses, timeout=120)
        command_output = self._blocking_command_responses.pop(command_type)
        logging.debug(f"Command {command_type} got response {command_output}")
        return command_output

    def grid_fees(self, fee_cents_kwh):
        logging.debug(f"Client tries to change grid fees.")
        self.redis_db.publish(f"{self._channel_prefix}/grid_fees", json.dumps({"fee_const": fee_cents_kwh}))
        return self._wait_and_consume_command_response("grid_fees")

    def change_grid_fees_percent(self, fee_percent):
        logging.debug(f"Client tries to change grid fees.")
        self.redis_db.publish(f"{self._channel_prefix}/grid_fees", json.dumps({"fee_percent": fee_percent}))
        return self._wait_and_consume_command_response("grid_fees")

    def last_market_dso_stats(self):
        logging.debug(f"Client tries to read dso_market_stats.")
        self.redis_db.publish(f"{self._channel_prefix}/dso_market_stats", json.dumps({}))
        return self._wait_and_consume_command_response("dso_market_stats")

    def _on_event_or_response(self, msg):
        message = json.loads(msg["data"])
        logging.info(f"A new message was received. Message information: {message}")
        log_market_progression(message)
        function = lambda: self.on_event_or_response(message)
        self.executor.submit(execute_function_util, function=function,
                             function_name="on_event_or_response")

    def _on_market_cycle(self, msg):
        message = json.loads(msg["data"])
        function = lambda: self.on_market_cycle(message)
        self.executor.submit(execute_function_util, function=function,
                             function_name="on_market_cycle")

    def on_market_cycle(self, market_info):
        pass

    def _on_finish(self, msg):
        message = json.loads(msg["data"])
        function = lambda: self.on_finish(message)

        self.executor.submit(execute_function_util, function=function,
                             function_name="on_finish")

    def on_finish(self, finish_info):
        pass

    def on_event_or_response(self, message):
        pass
