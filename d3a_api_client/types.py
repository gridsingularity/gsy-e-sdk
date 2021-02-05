import os
from d3a_api_client.rest_device import RestDeviceClient
from d3a_api_client.redis_device import RedisDeviceClient
from d3a_api_client.aggregator import Aggregator
from d3a_api_client.redis_aggregator import RedisAggregator
from d3a_api_client.rest_market import RestMarketClient
from d3a_api_client.redis_market import RedisMarketClient


def _select_client_type(rest_type, redis_type):
    if "API_CLIENT_RUN_ON_REDIS" in os.environ and \
            os.environ["API_CLIENT_RUN_ON_REDIS"] == "true":
        return redis_type
    else:
        return rest_type


device_client_type = _select_client_type(RestDeviceClient, RedisDeviceClient)


aggregator_client_type = _select_client_type(Aggregator, RedisAggregator)


market_client_type = _select_client_type(RestMarketClient, RedisMarketClient)
