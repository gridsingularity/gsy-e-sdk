import json
import logging
from time import sleep

from gsy_framework.utils import key_in_dict_and_not_none_and_greater_than_zero

from gsy_e_sdk.aggregator import Aggregator

from gsy_e_sdk.clients.rest_asset_client import RestAssetClient
from gsy_e_sdk.utils import (
    get_area_uuid_from_area_name_and_collaboration_id, get_sim_id_and_domain_names)


class TestAggregator(Aggregator):
    """Aggregator that automatically reacts on market cycle events with bids and offers."""

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

        for area_uuid, area_dict in self.latest_grid_tree_flat.items():

            if area_dict.get("asset_info"):
                if key_in_dict_and_not_none_and_greater_than_zero(area_dict["asset_info"],
                                                                  "available_energy_kWh"):
                    energy = area_dict["asset_info"]["available_energy_kWh"] / 2
                    self.add_to_batch_commands.offer_energy(asset_uuid=area_uuid, price=1,
                                                            energy=energy)
                    logging.info("Energy OFFER command added to batch.")

                if key_in_dict_and_not_none_and_greater_than_zero(area_dict["asset_info"],
                                                                  "energy_requirement_kWh"):
                    energy = area_dict["asset_info"]["energy_requirement_kWh"] / 2
                    self.add_to_batch_commands.bid_energy(asset_uuid=area_uuid, price=30,
                                                          energy=energy)
                    logging.info("Energy BID command added to batch.")

        response = self.execute_batch_commands()
        logging.info("Batch command placed on the new market: %s", response)

    def on_tick(self, tick_info):
        logging.debug("Progress information on the device: %s", tick_info)
        for area_uuid, area_dict in self.latest_grid_tree_flat.items():
            if "asset_info" not in area_dict or area_dict["asset_info"] is None:
                continue

            # Information logging
            logging.info(f"area_uuid = \n {json.dumps(area_uuid, indent=3)}")
            logging.info(f"area_dict = \n {json.dumps(area_dict, indent=3)}")

    def on_trade(self, trade_info):
        logging.debug("Trade info: %s", trade_info)

    def on_finish(self, finish_info):
        self.is_finished = True


simulation_id, domain_name, websockets_domain_name = get_sim_id_and_domain_names()

aggr = TestAggregator(aggregator_name="oracle_sarah_aggregator_s4_2")

load2_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    simulation_id, "Load_S4_2", domain_name)
load2 = RestAssetClient(load2_uuid)

pv2_uuid = get_area_uuid_from_area_name_and_collaboration_id(
    simulation_id, "PV_S4_2", domain_name)
pv2 = RestAssetClient(pv2_uuid)

load2.select_aggregator(aggr.aggregator_uuid)
pv2.select_aggregator(aggr.aggregator_uuid)
# load2.unselect_aggregator(aggr.aggregator_uuid)
# pv2.unselect_aggregator(aggr.aggregator_uuid)

print("'Load_S4_2' & 'PV_S4_2' assets have been registered.")

while not aggr.is_finished:
    sleep(0.5)
