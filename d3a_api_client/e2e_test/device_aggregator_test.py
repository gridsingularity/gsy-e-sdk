import logging
from d3a_interface.utils import key_in_dict_and_not_none_and_greater_than_zero
from time import sleep

from d3a_api_client.aggregator import Aggregator
from d3a_api_client.rest_device import RestDeviceClient
from d3a_api_client.utils import (
    get_area_uuid_from_area_name_and_collaboration_id, get_sim_id_and_domain_names)

class TestAggregator(Aggregator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False

    def on_market_cycle(self, market_info):
        """
        Places a bid or an offer whenever a new market is created. The amount of energy
        for the bid/offer depends on the available energy of the PV and storage, or on the required
        energy of the load or storage
        :param market_info: Incoming message containing the newly-created market info
        :return: None
        """
        if self.is_finished is True:
            return
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                continue
            #For PV
            if key_in_dict_and_not_none_and_greater_than_zero(area_dict["asset_info"], "available_energy_kWh"):
                self.add_to_batch_commands.offer_energy(area_uuid=area_uuid, price=29,
                                                        energy = area_dict["asset_info"]["available_energy_kWh"] / 2)
            #For Load
            if key_in_dict_and_not_none_and_greater_than_zero(area_dict["asset_info"], "energy_requirement_kWh"):
                self.add_to_batch_commands.bid_energy(area_uuid=area_uuid, price=31,
                                                      energy = area_dict["asset_info"]["energy_requirement_kWh"] / 2)
            #For Storage offer
            if key_in_dict_and_not_none_and_greater_than_zero(area_dict["asset_info"], "energy_to_sell"):
                self.add_to_batch_commands.offer_energy(area_uuid=area_uuid, price=28,
                                                        energy = area_dict["asset_info"]["energy_to_sell"] / 2)
            #For Storage bid
            if key_in_dict_and_not_none_and_greater_than_zero(area_dict["asset_info"], "energy_to_buy"):
                self.add_to_batch_commands.bid_energy(area_uuid=area_uuid, price=31,
                                                      energy = area_dict["asset_info"]["energy_to_buy"] / 2) 
 	                                                             

        response = self.execute_batch_commands()
        logging.info(f"Batch command placed on the new market: {response}")

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

load_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    simulation_id, 'Load', domain_name)
device_args['device_id'] = load_uuid
load = RestDeviceClient(
    **device_args
)

pv_uuid = get_area_uuid_from_area_name_and_collaboration_id(simulation_id, 'PV', domain_name)
device_args['device_id'] = pv_uuid
pv = RestDeviceClient(
    **device_args
)

storage_uuid = get_area_uuid_from_area_name_and_collaboration_id(simulation_id, 'Storage', domain_name)
device_args['device_id'] = storage_uuid
storage = RestDeviceClient(
    **device_args
)

#select the aggregator
load.select_aggregator(aggr.aggregator_uuid)
pv.select_aggregator(aggr.aggregator_uuid)
storage.select_aggregator(aggr.aggregator_uuid)


while not aggr.is_finished:
    sleep(0.5)
