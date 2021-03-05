import os

from websocket_subscriber.live_data_consumer import LiveData
from d3a_api_client.utils import domain_name_from_env, consumer_websocket_domain_name_from_env


def create_live_data_consumer():
    live_data = LiveData()
