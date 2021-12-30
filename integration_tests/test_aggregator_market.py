import logging
import traceback

from integration_tests.test_aggregator_base import TestAggregatorBase
from gsy_e_sdk.redis_market import RedisMarketClient


class MarketAggregator(TestAggregatorBase):
    """Test Aggregator for market connections."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.grid_fee_cents_kwh = 5
        self._has_tested_market = False

    def _setup(self):
        self.house_market = RedisMarketClient("house-2")
        self.house_market.select_aggregator(self.aggregator_uuid)

    def on_market_cycle(self, market_info):
        logging.info("market_info: %s", market_info)
        try:
            for area_uuid in self.latest_grid_tree_flat:
                if area_uuid == self.house_market.area_uuid:
                    self.add_to_batch_commands.grid_fees(area_uuid=self.house_market.area_uuid,
                                                         fee_cents_kwh=self.grid_fee_cents_kwh)
                    self.add_to_batch_commands.last_market_dso_stats(self.house_market.area_uuid)

                    transactions = self.send_batch_commands()
                    if transactions:
                        grid_fee_requests = self._filter_commands_from_responses(
                            transactions["responses"], "grid_fees")
                        assert len(grid_fee_requests) == 1
                        assert float(grid_fee_requests[0]["market_fee_const"]) == \
                               self.grid_fee_cents_kwh

                        stats_requests = self._filter_commands_from_responses(
                            transactions["responses"], "dso_market_stats")
                        assert len(stats_requests) == 1
                        assert set(stats_requests[0]["market_stats"]) == \
                            {"total_traded_energy_kWh", "max_trade_rate", "min_trade_rate",
                             "avg_trade_rate", "market_bill", "market_fee_revenue",
                             "self_sufficiency", "median_trade_rate", "self_consumption",
                             "market_energy_deviance", "area_throughput"}
                        self._has_tested_market = True

        except Exception as ex:  # pylint: disable=broad-except
            error_message = f"Raised exception: {ex}. Traceback: {traceback.format_exc()}"
            logging.error(error_message)
            self.errors.append(error_message)

    def on_finish(self, finish_info):
        # Make sure that all test cases have been run
        if self._has_tested_market is False:
            error_message = (
                "Not all test cases have been covered. This will be reported as failure.")
            logging.error(error_message)
            self.errors.append(error_message)

        self.status = "finished"
