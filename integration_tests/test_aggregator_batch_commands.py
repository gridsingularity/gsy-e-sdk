import json
import logging
from math import isclose
import traceback

from d3a_interface.utils import key_in_dict_and_not_none

from d3a_api_client.enums import Commands, command_enum_to_command_name
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

    def on_market_cycle(self, market_info):
        logging.info(f"market_info: {market_info}")
        try:
            if self.initial_grid_fees_market_cycle == {} and \
                    self.grid_fee_calculation.latest_grid_stats_tree:
                for target_market in ['Grid', 'House 1', 'House 2']:
                    self.initial_grid_fees_market_cycle[target_market] = self.calculate_grid_fee(
                        'load', target_market, 'last_market_fee')

            for device_event in market_info['content']:
                if 'device_info' not in device_event or device_event['device_info'] is None:
                    continue
                if key_in_dict_and_not_none(device_event, 'grid_stats_tree'):
                    json_grid_tree = json.dumps(device_event['grid_stats_tree'], indent=2)
                    logging.warning(json_grid_tree)

                if self._can_place_offer(device_event):
                    self.add_to_batch_commands.offer_energy(
                        area_uuid=device_event['area_uuid'],
                        price=1.1,
                        energy=device_event['device_info']['available_energy_kWh'] / 4,
                        replace_existing=False
                    ).offer_energy(
                        area_uuid=device_event['area_uuid'],
                        price=2.2,
                        energy=device_event['device_info']['available_energy_kWh'] / 4,
                        replace_existing=False
                    ).offer_energy(
                        area_uuid=device_event['area_uuid'],
                        price=3.3,
                        energy=device_event['device_info']['available_energy_kWh'] / 4,
                        replace_existing=True
                    ).offer_energy(
                        area_uuid=device_event['area_uuid'],
                        price=4.4,
                        energy=device_event['device_info']['available_energy_kWh'] / 4,
                        replace_existing=False
                    ).list_offers(area_uuid=device_event['area_uuid'])

                if self._can_place_bid(device_event):
                    self.add_to_batch_commands.bid_energy(
                        area_uuid=device_event['area_uuid'],
                        price=27,
                        energy=device_event['device_info']['energy_requirement_kWh'] / 4,
                        replace_existing=False
                    ).bid_energy(
                        area_uuid=device_event['area_uuid'],
                        price=28,
                        energy=device_event['device_info']['energy_requirement_kWh'] / 4,
                        replace_existing=False
                    ).bid_energy(
                        area_uuid=device_event['area_uuid'],
                        price=29,
                        energy=device_event['device_info']['energy_requirement_kWh'] / 4,
                        replace_existing=True
                    ).bid_energy(
                        area_uuid=device_event['area_uuid'],
                        price=30,
                        energy=device_event['device_info']['energy_requirement_kWh'] / 4,
                        replace_existing=False
                    ).list_bids(
                        area_uuid=device_event['area_uuid'])

                self.add_to_batch_commands.grid_fees(area_uuid=self.redis_market.area_uuid,
                                                     fee_cents_kwh=self.updated_house2_grid_fee_cents_kwh)
                self.add_to_batch_commands.last_market_dso_stats(self.redis_market.area_uuid)
                self.add_to_batch_commands.last_market_stats(self.redis_market.area_uuid)

            if self.commands_buffer_length:
                transaction = self.execute_batch_commands()
                if transaction is None:
                    self.errors += 1
                else:
                    for response in transaction["responses"]:
                        for area_response in response:
                            if area_response['status'] == 'error':
                                self.errors += 1

                logging.info(f'Batch command placed on the new market')

                # Make assertions about the bids, if they happened during this slot
                bid_requests = self._filter_commands_from_responses(
                    transaction['responses'][0], 'bid')
                if bid_requests:
                    # All bids in the batch have been issued
                    assert len(bid_requests) == 4
                    # All bids have been successfully received and processed
                    assert all(bid.get('status') == 'ready' for bid in bid_requests)

                    list_bids_requests = self._filter_commands_from_responses(
                        transaction['responses'][0], 'list_bids')

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
                    transaction['responses'][0], 'offer')
                if offer_requests:
                    # All offers in the batch have been issued
                    assert len(offer_requests) == 4
                    # All offers have been successfully received and processed
                    assert all(offer.get('status') == 'ready' for offer in offer_requests)

                    list_offers_requests = self._filter_commands_from_responses(
                        transaction['responses'][0], 'list_offers')

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

            for target_market in ["Grid", "House 1", "House 2"]:
                self.grid_fees_market_cycle_next_market[
                    target_market] = self.calculate_grid_fee("load", target_market)

        except Exception as ex:
            logging.error(f'Raised exception: {ex}. Traceback: {traceback.format_exc()}')
            self.errors += 1

    @staticmethod
    def _filter_commands_from_responses(responses, command_name):
        return [resp for resp in responses if resp.get('command') == command_name]

    @staticmethod
    def _can_place_bid(event):
        return (
            'energy_requirement_kWh' in event['device_info'] and
            event['device_info']['energy_requirement_kWh'] > 0.0)

    @staticmethod
    def _can_place_offer(event):
        return (
            'available_energy_kWh' in event['device_info'] and
            event['device_info']['available_energy_kWh'] > 0.0)

    def on_tick(self, tick_info):
        for target_market in ["Grid", "House 1", "House 2"]:
            self.grid_fees_tick_last_market[target_market] = self.calculate_grid_fee("load", target_market, "last_market_fee")

    def on_finish(self, finish_info):
        # Make sure that all test cases have been run
        if self._has_tested_bids is False or self._has_tested_offers is False:
            logging.error(
                'Not all test cases have been covered. This will be reported as failure.')
            self.errors += 1

        self.status = 'finished'
