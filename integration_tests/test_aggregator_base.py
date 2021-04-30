import logging

from d3a_api_client.redis_aggregator import RedisAggregator


class TestAggregatorBase(RedisAggregator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.errors = 0
        self.status = "running"
        self._setup()
        self.is_active = True
        self._has_tested_bids = False
        self._has_tested_offers = False

    def _setup(self):
        pass

    def on_market_cycle(self, market_info):
        pass

    def send_batch_commands(self):
        if self.commands_buffer_length:
            transaction = self.execute_batch_commands()
            if transaction is None:
                self.errors += 1
            else:
                for response in transaction["responses"].values():
                    for command_dict in response:
                        if command_dict["status"] == "error":
                            self.errors += 1
            logging.info(f"Batch command placed on the new market")
            return transaction

    @staticmethod
    def _filter_commands_from_responses(responses, command_name):
        filtered_commands = []
        for area_uuid, response in responses.items():
            for command_dict in response:
                if command_dict["command"] == command_name:
                    filtered_commands.append(command_dict)
        return filtered_commands

    @staticmethod
    def _can_place_bid(asset_info):
        return (
            "energy_requirement_kWh" in asset_info and
            asset_info["energy_requirement_kWh"] > 0.0)

    @staticmethod
    def _can_place_offer(asset_info):
        return (
            "available_energy_kWh" in asset_info and
            asset_info["available_energy_kWh"] > 0.0)

    def on_finish(self, finish_info):
        # Make sure that all test cases have been run
        if self._has_tested_bids is False or self._has_tested_offers is False:
            logging.error(
                "Not all test cases have been covered. This will be reported as failure.")
            self.errors += 1

        self.status = "finished"
