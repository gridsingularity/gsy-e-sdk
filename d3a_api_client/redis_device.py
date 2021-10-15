from d3a_api_client.constants import LOCAL_REDIS_URL
from d3a_api_client.redis_client_base import RedisClientBase


class RedisDeviceClient(RedisClientBase):
    """
    Class is kept for backward compatibility and also for following the same approach as in the
    REST case to have two different classes for devices and markets
    """
    def __init__(self, area_id, autoregister=True, redis_url=LOCAL_REDIS_URL,
                 pubsub_thread=None):
        super().__init__(area_id, autoregister, redis_url, pubsub_thread)

    def register(self, is_blocking=True):
        super().register(is_blocking)

    def unregister(self, is_blocking=True):
        super().unregister(is_blocking)

    def on_event_or_response(self, message):
        pass
