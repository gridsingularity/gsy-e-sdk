import logging
import os
from time import sleep

from pendulum import today

from d3a_api_client.aggregator import Aggregator
from d3a_api_client.rest_device import RestDeviceClient
<<<<<<< HEAD
from d3a_api_client.utils import get_area_uuid_from_area_name_and_collaboration_id
=======
from d3a_api_client.utils import get_area_uuid_from_area_name_and_collaboration_id, \
    get_simulation_config
from d3a_interface.constants_limits import DATE_TIME_FORMAT
>>>>>>> master
from d3a_api_client.rest_market import RestMarketClient
from d3a_interface.utils import key_in_dict_and_not_none_and_greater_than_zero


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
<<<<<<< HEAD
        market_uuid = self.get_uuid_from_area_name("Market")
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                continue

            logging.info(
                f"current_market_fee: {self.grid_fee_calculation.calculate_grid_fee(area_uuid, market_uuid)}")
            if key_in_dict_and_not_none_and_greater_than_zero(area_dict, "available_energy_kWh"):
                self.add_to_batch_commands.offer_energy(area_uuid=area_uuid, price=1,
                                                        energy=area_dict["asset_info"]["available_energy_kWh"] / 2) \
                    .list_offers(area_uuid=area_uuid)
            if key_in_dict_and_not_none_and_greater_than_zero(area_dict, "energy_requirement_kWh"):
                self.add_to_batch_commands.bid_energy(area_uuid=area_uuid, price=30,
                                                      energy=area_dict["asset_info"]["energy_requirement_kWh"] / 2) \
                    .list_bids(area_uuid=area_uuid)
        response = self.execute_batch_commands()
        logging.info(f"Batch command placed on the new market: {response}")
=======
        if 'content' not in market_info:
            return

        for device_event in market_info['content']:
            if key_in_dict_and_not_none_and_greater_than_zero(
                    device_event, 'available_energy_kWh'):
                self.add_to_batch_commands.\
                    offer_energy(device_event['area_uuid'], price=1,
                                 energy=device_event['device_info']['available_energy_kWh'] / 2)
                self.add_to_batch_commands.list_offers(device_event['area_uuid'])

            if key_in_dict_and_not_none_and_greater_than_zero(
                    device_event, 'energy_requirement_kWh'):
                self.add_to_batch_commands.\
                    bid_energy(device_event['area_uuid'], price=30,
                               energy=device_event['device_info']['energy_requirement_kWh'] / 2)
                self.add_to_batch_commands.list_bids(device_event['area_uuid'])

            response = self.execute_batch_commands()
            logging.debug(f'Batch command placed on the new market: {response}')
>>>>>>> master

    def on_tick(self, tick_info):
        logging.debug(f'Progress information on the device: {tick_info}')

    def on_trade(self, trade_info):
        logging.debug(f'Trade info: {trade_info}')

    def on_finish(self, finish_info):
        self.is_finished = True


<<<<<<< HEAD
simulation_id = os.environ["API_CLIENT_SIMULATION_ID"]
domain_name = os.environ["API_CLIENT_DOMAIN_NAME"]
websocket_domain_name = os.environ["API_CLIENT_WEBSOCKET_DOMAIN_NAME"]
=======
simulation_id, domain_name, websocket_domain_name = get_simulation_config()
>>>>>>> master

aggr = TestAggregator(
    aggregator_name='test_aggr',
)

device_args = {
    'autoregister': False,
    'start_websocket': False
}

load1_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    simulation_id, 'Load', domain_name)
device_args['device_id'] = load1_uuid


load1 = RestDeviceClient(
    **device_args
)

<<<<<<< HEAD
pv1_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    device_args["simulation_id"], "PV", device_args["domain_name"])
device_args["device_id"] = pv1_uuid
=======

load2_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    device_args['simulation_id'], 'Load 2', device_args['domain_name'])
device_args['device_id'] = load2_uuid

load2 = RestDeviceClient(
    **device_args
)

pv1_uuid = get_area_uuid_from_area_name_and_collaboration_id(simulation_id, 'PV', domain_name)
device_args['device_id'] = pv1_uuid
>>>>>>> master
pv1 = RestDeviceClient(
    **device_args
)

load1.select_aggregator(aggr.aggregator_uuid)
pv1.select_aggregator(aggr.aggregator_uuid)

<<<<<<< HEAD
house_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    simulation_id, "House", domain_name)
rest_market = RestMarketClient(simulation_id, house_uuid, domain_name, websocket_domain_name)
rest_market.select_aggregator(aggr.aggregator_uuid)
=======
area_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    simulation_id, 'House', domain_name)

rest_market = RestMarketClient(area_uuid, simulation_id, domain_name, websocket_domain_name)
market_slot_string = today().add(minutes=60).format(DATE_TIME_FORMAT)
last_market_stats = rest_market.last_market_stats()
>>>>>>> master

while not aggr.is_finished:
    sleep(0.5)
