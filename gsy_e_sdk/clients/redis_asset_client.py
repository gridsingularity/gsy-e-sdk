from gsy_e_sdk.constants import LOCAL_REDIS_URL
from gsy_e_sdk.redis_client_base import RedisClientBase


class RedisAssetClient(RedisClientBase):
    """Client class to connect assets while working with Redis.

    This class is equivalent to the RedisClientBase. However, it is implemented in order to follow
    the same approach as in the REST case, where we have two different classes for devices and
    markets.
    """

    def __init__(self, asset_uuid, autoregister=True, redis_url=LOCAL_REDIS_URL,
                 pubsub_thread=None):
        super().__init__(asset_uuid, autoregister, redis_url, pubsub_thread)
