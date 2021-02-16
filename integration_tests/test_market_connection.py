"""
Test file for the device client. Depends on d3a test setup file strategy_tests.external_devices
"""
from d3a_api_client.types import market_client_type


class AutoLastMarketStats(market_client_type):
    def __init__(self, *args, **kwargs):
        self.errors = 0
        self.error_list = []
        self.status = "running"
        self.latest_stats = {}
        self.market_info = {}
        self.device_bills = {}
        super().__init__(*args, **kwargs)
        self.wait_script = True

    def on_finish(self, message):
        self.wait_script = False
        self.status = "finished"
