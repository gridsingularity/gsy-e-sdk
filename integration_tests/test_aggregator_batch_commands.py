import logging

import json
import traceback

from d3a_api_client.redis_aggregator import RedisAggregator
from d3a_api_client.redis_device import RedisDeviceClient
from d3a_api_client.redis_market import RedisMarketClient


class BatchAggregator(RedisAggregator):
    def __init__(self, *args, **kwargs):
        self.grid_fees_market_cycle_next_market = {}
        self.grid_fees_tick_last_market = {}
        self.initial_grid_fees_market_cycle = {}
        super().__init__(*args, **kwargs)
        self.errors = 0
        self.status = "running"
        self._setup()
        self.is_active = True
        self.updated_house2_grid_fee_cents_kwh = 5
        self.updated_offer_bid_price = 60

        self._has_tested_bids = False
        self._has_tested_offers = False

    def _setup(self):
        load = RedisDeviceClient('load', autoregister=True)
        pv = RedisDeviceClient('pv', autoregister=True)

        load.select_aggregator(self.aggregator_uuid)
        pv.select_aggregator(self.aggregator_uuid)

        self.redis_market = RedisMarketClient('house-2')
        self.redis_market.select_aggregator(self.aggregator_uuid)

    def require_grid_fees(self, grid_fee_buffer_dict, fee_type):
        if self.area_name_uuid_mapping:
            load_uuid = self.get_uuid_from_area_name("load")
            for target_market in ["Grid", "House 1", "House 2"]:
                market_uuid = self.get_uuid_from_area_name(target_market)
                grid_fee_buffer_dict[target_market] = \
                    self.calculate_grid_fee(load_uuid, market_uuid, fee_type)

    def on_market_cycle(self, market_info):
        logging.info(f"market_info: {market_info}")
        try:
            if self.initial_grid_fees_market_cycle == {} and \
                    self.grid_fee_calculation.latest_grid_stats_tree != {}:
                self.require_grid_fees(self.initial_grid_fees_market_cycle, "last_market_fee")

            for area_uuid, area_dict in self.latest_grid_tree_flat.items():
                if area_uuid == self.redis_market.area_uuid:
                    self.add_to_batch_commands.grid_fees(area_uuid=self.redis_market.area_uuid,
                                                         fee_cents_kwh=self.updated_house2_grid_fee_cents_kwh)
                    self.add_to_batch_commands.last_market_dso_stats(self.redis_market.area_uuid)
                    self.add_to_batch_commands.last_market_stats(self.redis_market.area_uuid)
                if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                    continue
                asset_info = area_dict["asset_info"]
                if self._can_place_offer(asset_info):
                    self.add_to_batch_commands.offer_energy(
                        area_uuid=area_uuid,
                        price=1.1,
                        energy=asset_info['available_energy_kWh'] / 4,
                        replace_existing=False
                    ).offer_energy(
                        area_uuid=area_uuid,
                        price=2.2,
                        energy=asset_info['available_energy_kWh'] / 4,
                        replace_existing=False
                    ).offer_energy(
                        area_uuid=area_uuid,
                        price=3.3,
                        energy=asset_info['available_energy_kWh'] / 4,
                        replace_existing=True
                    ).offer_energy(
                        area_uuid=area_uuid,
                        price=4.4,
                        energy=asset_info['available_energy_kWh'] / 4,
                        replace_existing=False
                    ).list_offers(area_uuid=area_uuid)

                if self._can_place_bid(asset_info):
                    self.add_to_batch_commands.bid_energy(
                        area_uuid=area_uuid,
                        price=27,
                        energy=asset_info['energy_requirement_kWh'] / 4,
                        replace_existing=False
                    ).bid_energy(
                        area_uuid=area_uuid,
                        price=28,
                        energy=asset_info['energy_requirement_kWh'] / 4,
                        replace_existing=False
                    ).bid_energy(
                        area_uuid=area_uuid,
                        price=29,
                        energy=asset_info['energy_requirement_kWh'] / 4,
                        replace_existing=True
                    ).bid_energy(
                        area_uuid=area_uuid,
                        price=30,
                        energy=asset_info['energy_requirement_kWh'] / 4,
                        replace_existing=False
                    ).list_bids(
                        area_uuid=area_uuid)

            if self.commands_buffer_length:
                transaction = self.execute_batch_commands()
                if transaction is None:
                    self.errors += 1
                else:
                    for area_uuid, response in transaction["responses"].items():
                        for command_dict in response:
                            if command_dict["status"] == "error":
                                self.errors += 1
                logging.info(f"Batch command placed on the new market")

                # Make assertions about the bids, if they happened during this slot
                bid_requests = self._filter_commands_from_responses(
                    transaction['responses'], 'bid')
                if bid_requests:
                    # All bids in the batch have been issued
                    assert len(bid_requests) == 4
                    # All bids have been successfully received and processed
                    assert all(bid.get('status') == 'ready' for bid in bid_requests)

                    list_bids_requests = self._filter_commands_from_responses(
                        transaction['responses'], 'list_bids')

                    # The list_bids command has been issued once
                    assert len(list_bids_requests) == 1

                    # The bid list only contains two bids (the other two have been deleted)
                    current_bids = list_bids_requests[0]['bid_list']
                    assert len(current_bids) == 2

                    issued_bids = [json.loads(bid_request['bid']) for bid_request in bid_requests]

                    # The bids have been issued in the correct order
                    assert [
                               bid['original_bid_price'] for bid in issued_bids
                           ] == [27, 28, 29, 30]

                    # The only two bids left are the last ones that have been issued
                    assert [bid['id'] for bid in current_bids] == \
                           [bid['id'] for bid in issued_bids[-2:]]

                    self._has_tested_bids = True

                # Make assertions about the offers, if they happened during this slot
                offer_requests = self._filter_commands_from_responses(
                    transaction['responses'], 'offer')
                if offer_requests:
                    # All offers in the batch have been issued
                    assert len(offer_requests) == 4
                    # All offers have been successfully received and processed
                    assert all(offer.get('status') == 'ready' for offer in offer_requests)

                    list_offers_requests = self._filter_commands_from_responses(
                        transaction['responses'], 'list_offers')

                    # The list_offers command has been issued once
                    assert len(list_offers_requests) == 1

                    # The offers list only contains two offers (the other two have been deleted)
                    current_offers = list_offers_requests[0]['offer_list']
                    assert len(current_offers) == 2

                    issued_offers = [
                        json.loads(offer_request['offer']) for offer_request in offer_requests]

                    # The offers have been issued in the correct order
                    assert [
                               offer['original_offer_price'] for offer in issued_offers
                           ] == [1.1, 2.2, 3.3, 4.4]

                    # The only two offers left are the last ones that have been issued
                    assert [offer['id'] for offer in current_offers] == \
                           [offer['id'] for offer in issued_offers[-2:]]

                    self._has_tested_offers = True

                market_stats_requests_responses = self._filter_commands_from_responses(
                    transaction['responses'], 'dso_market_stats')
                if market_stats_requests_responses:
                    assert set(market_stats_requests_responses[0]["market_stats"].keys()) == {
                        "min_trade_rate", "max_trade_rate", "avg_trade_rate", "median_trade_rate",
                        "total_traded_energy_kWh", "market_bill", "market_fee_revenue",
                        "area_throughput", "self-sufficiency", "self_consumption"}

            self.require_grid_fees(self.grid_fees_market_cycle_next_market, "current_market_fee")

        except Exception as ex:
            logging.error(f'Raised exception: {ex}. Traceback: {traceback.format_exc()}')
            self.errors += 1

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
            'energy_requirement_kWh' in asset_info and
            asset_info['energy_requirement_kWh'] > 0.0)

    @staticmethod
    def _can_place_offer(asset_info):
        return (
            'available_energy_kWh' in asset_info and
            asset_info['available_energy_kWh'] > 0.0)

    def on_tick(self, tick_info):
        self.require_grid_fees(self.grid_fees_tick_last_market, "last_market_fee")

    def on_finish(self, finish_info):
        # Make sure that all test cases have been run
        if self._has_tested_bids is False or self._has_tested_offers is False:
            logging.error(
                'Not all test cases have been covered. This will be reported as failure.')
            self.errors += 1

        self.status = 'finished'
