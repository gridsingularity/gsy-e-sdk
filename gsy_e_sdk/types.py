import os
from gsy_e_sdk.clients.rest_asset_client import RestAssetClient
from gsy_e_sdk.clients.redis_asset_client import RedisAssetClient
from gsy_e_sdk.aggregator import Aggregator
from gsy_e_sdk.redis_aggregator import RedisAggregator
from gsy_e_sdk.rest_market import RestMarketClient
from gsy_e_sdk.redis_market import RedisMarketClient


def _select_client_type(rest_type, redis_type):
    if "API_CLIENT_RUN_ON_REDIS" in os.environ and os.environ["API_CLIENT_RUN_ON_REDIS"] == "true":
        return redis_type

    return rest_type


# pylint: disable=invalid-name
device_client_type = _select_client_type(RestAssetClient, RedisAssetClient)
aggregator_client_type = _select_client_type(Aggregator, RedisAggregator)
market_client_type = _select_client_type(RestMarketClient, RedisMarketClient)
