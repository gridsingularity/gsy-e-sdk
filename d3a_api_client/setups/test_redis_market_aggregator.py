import logging
from time import sleep
from d3a_api_client.redis_aggregator import RedisAggregator
from d3a_api_client.redis_market import RedisMarketClient


class AutoAggregator(RedisAggregator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False
        self.fee_cents_per_kwh = 0

    def on_market_cycle(self, market_info):
        """
        market_info contains market_info dicts from all markets
        that are controlled by the aggregator
        """
        logging.info(f"AGGREGATOR_MARKET_INFO: {market_info}")
        self.fee_cents_per_kwh += 1
        for area_event in market_info["content"]:
            area_uuid = area_event["area_uuid"]
            if area_uuid is None:
                continue
            self.add_to_batch_commands.last_market_dso_stats(area_uuid=area_uuid).\
                grid_fees(area_uuid=area_uuid, fee_cents_kwh=self.fee_cents_per_kwh)

        if self.len_commands_buffer:
            response = self.execute_batch_commands(is_blocking=True)
            logging.warning(f"Batch command placed on the new market: {response}")
        logging.info(f"---------------------------")

    def on_finish(self, finish_info):
        self.is_finished = True
        logging.info(f"AGGREGATOR_FINISH_INFO: {finish_info}")

    def on_batch_response(self, market_stats):
        logging.info(f"AGGREGATORS_BATCH_RESPONSE: {market_stats}")


aggregator = AutoAggregator(
    aggregator_name="aggregator"
)

house_1 = RedisMarketClient("house-1")
selected = house_1.select_aggregator(aggregator.aggregator_uuid)
logging.warning(f"SELECTED: {selected}")
house_2 = RedisMarketClient("house-2")
selected = house_2.select_aggregator(aggregator.aggregator_uuid)
logging.warning(f"SELECTED: {selected}")

while not aggregator.is_finished:
    sleep(0.5)
