import logging
from time import sleep
import json
from d3a_api_client.aggregator import Aggregator
from d3a_api_client.rest_device import RestDeviceClient
from d3a_api_client.utils import get_area_uuid_from_area_name_and_collaboration_id
from d3a_interface.constants_limits import DATE_TIME_FORMAT
from d3a_api_client.rest_market import RestMarketClient
from d3a_interface.utils import key_in_dict_and_not_none_and_greater_than_zero
from d3a_api_client.utils import get_sim_id_and_domain_names


class TestAggregator(Aggregator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_cycle(self, market_info):
        """
        Places a bid or an offer whenever a new market is created. The amount of energy
        for the bid/offer depends on the available energy of the PV, or on the required
        energy of the load.
        :param market_info: Incoming message containing the newly-created market info
        :return: None
        """
        if self.is_finished is True:
            return
        if 'content' not in market_info:
            return
        for device_event in market_info['content']:
            if 'device_info' not in device_event or device_event['device_info'] is None:
                continue
            # For PV
            if key_in_dict_and_not_none_and_greater_than_zero(
                    device_event['device_info'], 'available_energy_kWh'):
                pv_area_uuid = device_event['area_uuid']
                offered_energy = device_event['device_info']['available_energy_kWh'] / 2
                price_offered = 29
                self.add_to_batch_commands. \
                    offer_energy(pv_area_uuid, offered_energy,
                                 price_offered)
                self.add_to_batch_commands.list_offers(device_event['area_uuid'])
            # For Load
            if key_in_dict_and_not_none_and_greater_than_zero(
                    device_event['device_info'], 'energy_requirement_kWh'):
                load_area_uuid = device_event['area_uuid']
                bid_energy = device_event['device_info']['energy_requirement_kWh'] / 2
                bid_price = 31
                self.add_to_batch_commands. \
                    bid_energy(load_area_uuid, bid_energy,
                               bid_price)
                self.add_to_batch_commands.list_bids(device_event['area_uuid'])
            # For Storage offer
            if key_in_dict_and_not_none_and_greater_than_zero(
                    device_event['device_info'], 'energy_to_sell'):
                storage_area_uuid = device_event['area_uuid']
                offered_energy = device_event['device_info']['energy_to_sell'] / 2
                price_offered = 28
                self.add_to_batch_commands. \
                    offer_energy(storage_area_uuid, offered_energy,
                                 price_offered)
                self.add_to_batch_commands.list_bids(device_event['area_uuid'])
            # For Storage bid
            if key_in_dict_and_not_none_and_greater_than_zero(
                    device_event['device_info'], 'energy_to_buy'):
                storage_area_uuid = device_event['area_uuid']
                bid_energy = device_event['device_info']['energy_to_buy'] / 2
                bid_price = 31
                self.add_to_batch_commands. \
                    bid_energy(storage_area_uuid, bid_energy,
                               bid_price)
                self.add_to_batch_commands.list_bids(device_event['area_uuid'])
            response = self.execute_batch_commands()
            logging.info(f'Batch command placed on the new market: {response}')

    def on_tick(self, tick_info):
        logging.debug(f'Progress information on the device: {tick_info}')

    def on_trade(self, trade_info):
        logging.debug(f'Trade info: {trade_info}')

    def on_finish(self, finish_info):
        self.is_finished = True


simulation_id, domain_name, websockets_domain_name = get_sim_id_and_domain_names()
aggr = TestAggregator(
    aggregator_name='device_aggregator'
)
device_args = {
    'autoregister': False,
    'start_websocket': False
}
#For Load device
load1_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    simulation_id, 'Load', domain_name)
device_args['device_id'] = load1_uuid
load1 = RestDeviceClient(
    **device_args
)
#For PV device
pv1_uuid = get_area_uuid_from_area_name_and_collaboration_id(simulation_id, 'PV', domain_name)
device_args['device_id'] = pv1_uuid
pv1 = RestDeviceClient(
    **device_args
)
#For Storage device
storage_uuid = get_area_uuid_from_area_name_and_collaboration_id(simulation_id, 'Storage', domain_name)
device_args['device_id'] = storage_uuid
storage = RestDeviceClient(
    **device_args
)
#Connecting Load, PV and Storage with device aggregator.
load1.select_aggregator(aggr.aggregator_uuid)
pv1.select_aggregator(aggr.aggregator_uuid)
storage.select_aggregator(aggr.aggregator_uuid)
while not aggr.is_finished:
    sleep(0.5)
