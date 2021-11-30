import logging
from gsy_e_sdk.clients.redis_asset_client import RedisAssetClient

logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logger.addHandler(console_handler)

logger.warning(
    "The RedisDeviceClient class name will be deprecated. Please use RedisAssetClient instead.")


class RedisDeviceClient(RedisAssetClient):
    """Client class for assets to be used while working with Redis.

    Important: this class is deprecated. Please use RedisAssetClient instead.
    """
