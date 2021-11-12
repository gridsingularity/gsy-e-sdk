import logging
from gsy_e_sdk.clients.rest_asset_client import RestAssetClient

logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logger.addHandler(console_handler)

logger.warning(
    "The RestDeviceClient class name will be deprecated. Please use RedisAssetClient instead.")


class RestDeviceClient(RestAssetClient):
    """Client class for assets to be used while working with REST.

    Important: this class is deprecated. Please use RestAssetClient instead.
    """
