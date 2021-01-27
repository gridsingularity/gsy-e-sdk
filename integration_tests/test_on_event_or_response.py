"""
Test file for the device client. Depends on d3a test setup file strategy_tests.external_devices
"""
import json
import traceback
import logging
from d3a_api_client.redis_device import RedisDeviceClient


class OnEventOrResponse(RedisDeviceClient):
    def __init__(self, *args, **kwargs):
        self.errors = 0
        self.error_list = []
        self.status = "running"
        self.latest_stats = {}
        self.market_info = {}
        self.device_bills = {}
        super().__init__(*args, **kwargs)

    def on_market_cycle(self, market_info):
        pass

    def on_event_or_response(self, message):
        assert False
