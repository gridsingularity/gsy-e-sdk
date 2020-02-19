import logging
import json
from d3a_api_client.redis import RedisClient, Commands


class RedisDeviceClient(RedisClient):
    def __init__(self, device_id, autoregister=True, redis_url='redis://localhost:6379'):
        self.device_id = device_id
        super().__init__(device_id, None, autoregister, redis_url)
        self.is_active = True

    def _on_register(self, msg):
        message = json.loads(msg["data"])
        logging.info(f"Client was registered to the device: {message}")
        self.is_active = True

    def _on_unregister(self, msg):
        message = json.loads(msg["data"])
        logging.info(f"Client was unregistered from the device: {message}")
        self.is_active = False

    def _subscribe_to_response_channels(self):
        channel_subs = {
            f"{self._command_topics[c]}/response": self._generate_command_response_callback(c)
            for c in Commands
        }

        channel_subs[f'{self.market_id}/response/register_participant'] = self._on_register
        channel_subs[f'{self.market_id}/response/unregister_participant'] = self._on_unregister
        channel_subs[f'{self._channel_prefix}/market'] = self._on_market_cycle
        channel_subs[f'{self._channel_prefix}/tick'] = self._on_tick

        self.pubsub.subscribe(**channel_subs)
        self.pubsub.run_in_thread(daemon=True)

    @property
    def _channel_prefix(self):
        return f"{self.device_id}"
