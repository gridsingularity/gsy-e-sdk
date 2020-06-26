import logging
import json
import uuid
from d3a_api_client.redis_client_base import RedisClient, Commands
from d3a_interface.utils import wait_until_timeout_blocking


class RedisAPIException(Exception):
    pass


class RedisDeviceClient(RedisClient):
    def __init__(self, device_id, autoregister=True, redis_url='redis://localhost:6379'):
        self.device_id = device_id
        self._transaction_id_buffer = []
        super().__init__(device_id, None, autoregister, redis_url)

    def _on_register(self, msg):
        message = json.loads(msg["data"])
        self._check_buffer_message_matching_command_and_id(message)
        logging.info(f"Client was registered to the device: {message}")
        self.is_active = True

    def _on_unregister(self, msg):
        message = json.loads(msg["data"])
        self._check_buffer_message_matching_command_and_id(message)
        logging.info(f"Client was unregistered from the device: {message}")
        self.is_active = False

    def _subscribe_to_response_channels(self):
        channel_subs = {
            self._response_topics[c]: self._generate_command_response_callback(c)
            for c in Commands
        }

        channel_subs[f'{self.area_id}/response/register_participant'] = self._on_register
        channel_subs[f'{self.area_id}/response/unregister_participant'] = self._on_unregister
        channel_subs[f'{self._channel_prefix}/events/market'] = self._on_market_cycle
        channel_subs[f'{self._channel_prefix}/events/tick'] = self._on_tick
        channel_subs[f'{self._channel_prefix}/events/trade'] = self._on_trade
        channel_subs[f'{self._channel_prefix}/events/finish'] = self._on_finish
        channel_subs["crud_aggregator_response"] = self._aggregator_response_callback

        self.pubsub.subscribe(**channel_subs)
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

    def select_aggregator(self, aggregator_name, is_blocking=True):
        logging.info(f"Device: {self.device_id} is trying to select aggregator {aggregator_name}")

        transaction_id = str(uuid.uuid4())
        data = {"aggregator_name": aggregator_name,
                "device_name": self.device_id,
                "type": "SELECT",
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

    @property
    def _channel_prefix(self):
        return f"{self.device_id}"
