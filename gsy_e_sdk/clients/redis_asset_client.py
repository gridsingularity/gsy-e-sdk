from gsy_e_sdk.redis_client_base import RedisClientBase


class RedisAssetClient(RedisClientBase):
    """Client class to connect assets while working with Redis.

    This class is equivalent to the RedisClientBase. However, it is implemented in order to follow
    the same approach as in the REST case, where we have two different classes for devices and
    markets.
    """
