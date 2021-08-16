import logging
import sys
from d3a_interface.utils import key_in_dict_and_not_none_and_greater_than_zero
from time import sleep
from d3a_api_client.aggregator import Aggregator
from d3a_api_client.rest_device import RestDeviceClient
from d3a_api_client.utils import (
    get_area_uuid_from_area_name_and_collaboration_id, get_sim_id_and_domain_names)
class TestDeviceAggregator2(Aggregator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_finished = False
        self.access_revoked = False
    def on_market_cycle(self, market_info):
        """
        Places a bid or an offer whenever a new market is created. The amount of energy
        for the bid/offer depends on the available energy of the PV, or on the required
        energy of the load
        :param market_info: Incoming message containing the newly-created market info
        :return: None
        """
        if self.is_finished is True:
            return
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if "asset_info" not in area_dict or area_dict["asset_info"]:
                continue
            # For PV
            if key_in_dict_and_not_none_and_greater_than_zero(area_dict["asset_info"], "available_energy_kWh"):
                if area_uuid == pv_uuid:
                    self.add_to_batch_commands.offer_energy_rate(area_uuid=area_uuid, rate=29,
                                                                 energy=area_dict["asset_info"][
                                                                            "available_energy_kWh"] / 2)
            # For Load
            if key_in_dict_and_not_none_and_greater_than_zero(area_dict["asset_info"], "energy_requirement_kWh"):
                if area_uuid == load_uuid:
                    self.add_to_batch_commands.device_info(area_uuid=area_uuid). \
                        bid_energy_rate(area_uuid=area_uuid, rate=30,
                                        energy=area_dict["asset_info"]["energy_requirement_kWh"] / 2)
        if self.commands_buffer_length > 0:
            response = self.execute_batch_commands()
            logging.info(f"Batch command placed on the new market: {response}")
            if response is None:
                self.access_revoked = True
    def on_tick(self, tick_info):
        logging.debug(f'Progress information on the device: {tick_info}')
    def on_trade(self, trade_info):
        logging.debug(f'Trade info: {trade_info}')
    def on_finish(self, finish_info):
        self.is_finished = True
        logging.info("The client script is connected until the simulation has finished")
simulation_id, domain_name, websockets_domain_name = get_sim_id_and_domain_names()
aggr = TestDeviceAggregator2(
    aggregator_name='dev_aggregator2'
)
device_args = {
    'autoregister': False,
    'start_websocket': False
}
load_uuid = get_area_uuid_from_area_name_and_collaboration_id(simulation_id, 'Load 2', domain_name)
load = RestDeviceClient(load_uuid)
pv_uuid = get_area_uuid_from_area_name_and_collaboration_id(simulation_id, 'PV 2', domain_name)
pv = RestDeviceClient(pv_uuid)
load.select_aggregator(aggr.aggregator_uuid)
print("Load UUID: ", load_uuid)
print("Load is connected with aggregator : " + load.active_aggregator)
pv.select_aggregator(aggr.aggregator_uuid)
print("PV UUID: ", pv_uuid)
print("PV is connected with aggregator : " + pv.active_aggregator)
device_list = [load, pv]
if all(device.active_aggregator is None for device in device_list):
    logging.info(f"devices are not allowed to connect to the simulation")
    sys.exit(1)
while not aggr.is_finished:
    if aggr.access_revoked is True:
        logging.info(f"Connection with simulation has been terminated")
        sys.exit(1)
    sleep(0.5)