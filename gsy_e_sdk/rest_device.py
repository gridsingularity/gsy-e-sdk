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
    # pylint: disable=too-many-arguments
    def __init__(
            self, area_id, simulation_id=None, domain_name=None, websockets_domain_name=None,
            autoregister=False, start_websocket=True, sim_api_domain_name=None):
        super().__init__(
            asset_uuid=area_id, simulation_id=simulation_id, domain_name=domain_name,
            websockets_domain_name=websockets_domain_name,
            autoregister=False, start_websocket=True, sim_api_domain_name=sim_api_domain_name)
