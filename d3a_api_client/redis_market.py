from d3a_api_client.redis_client_base import RedisClientBase
from slugify import slugify


class RedisMarketClient(RedisClientBase):
    """
    Class is kept for backward compatibility and also for following the same approach as in the
    REST case to have two different classes for devices and markets
    """
    def __init__(self, area_id, redis_url='redis://localhost:6379', autoregister=True):
        # TODO: Homogenize channel names in markets and devices to use either
        #  slugified or normal area names
        area_id = slugify(area_id, to_lower=True)

        super().__init__(area_id, autoregister, redis_url)

    def register(self, is_blocking=True):
        super().register(is_blocking)

    def unregister(self, is_blocking=True):
        super().unregister(is_blocking)

    def on_event_or_response(self, message):
        pass
