import logging
import json
from d3a_api_client.redis import RedisClient


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

    @property
    def _channel_prefix(self):
        return f"{self.device_id}"


