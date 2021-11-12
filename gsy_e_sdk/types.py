import os
from gsy_e_sdk.rest_device import RestDeviceClient
from gsy_e_sdk.redis_device import RedisDeviceClient
from gsy_e_sdk.aggregator import Aggregator
from gsy_e_sdk.redis_aggregator import RedisAggregator
from gsy_e_sdk.rest_market import RestMarketClient
from gsy_e_sdk.redis_market import RedisMarketClient


def _select_client_type(rest_type, redis_type):
    if "API_CLIENT_RUN_ON_REDIS" in os.environ and os.environ["API_CLIENT_RUN_ON_REDIS"] == "true":
        return redis_type

    return rest_type


# pylint: disable=invalid-name
device_client_type = _select_client_type(RestDeviceClient, RedisDeviceClient)
aggregator_client_type = _select_client_type(Aggregator, RedisAggregator)
market_client_type = _select_client_type(RestMarketClient, RedisMarketClient)
