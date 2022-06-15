# flake8: noqa
# pylint: disable=duplicate-code

"""
Template file for markets management through the gsy-e-sdk api client using Rest.
"""
from time import sleep
from gsy_e_sdk.redis_aggregator import RedisAggregator
from gsy_e_sdk.redis_market import RedisMarketClient
from gsy_e_sdk.utils import log_grid_fees_information

# List of markets' names to be connected with the API
MARKET_NAMES = [
    "Grid",
    "Community",
]

ORACLE_NAME = "dso"
SLOT_LENGTH = 15  # leave as is


class Oracle(RedisAggregator):
    """Class to represent the Grid Operator client type."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_cycle(self, market_info):
        current_market_fee = {}
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if area_dict["area_name"] in MARKET_NAMES:
                self.add_to_batch_commands.last_market_dso_stats(area_uuid)
                current_market_fee[area_dict["area_name"]] = area_dict[
                    "current_market_fee"
                ]
        next_market_fee = self._set_new_market_fee()
        log_grid_fees_information(MARKET_NAMES, current_market_fee, next_market_fee)

    def _set_new_market_fee(self):
        """Return the market fees for each market for the next time slot."""
        next_market_fee = {}
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if area_dict["area_name"] in MARKET_NAMES:
                next_market_fee[area_dict["area_name"]] = 10

                self.add_to_batch_commands.grid_fees(
                    area_uuid=area_uuid,
                    fee_cents_kwh=next_market_fee[area_dict["area_name"]],
                )
        self.execute_batch_commands()
        return next_market_fee

    def on_event_or_response(self, message):
        pass

    def on_finish(self, finish_info):
        self.is_finished = True


MarketClient = RedisMarketClient
aggregator = Oracle(aggregator_name=ORACLE_NAME)

print()
print("Connecting to markets ...")

for i in MARKET_NAMES:
    market_registered = RedisMarketClient(area_id=i)
    market_registered.select_aggregator(aggregator.aggregator_uuid)
    print("----> Connected to ", i)

print(aggregator.device_uuid_list)

# loop to allow persistence
while not aggregator.is_finished:
    sleep(0.5)
