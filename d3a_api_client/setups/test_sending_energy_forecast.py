import logging
from time import sleep

from d3a_api_client.aggregator import Aggregator
from d3a_api_client.rest_device import RestDeviceClient
from d3a_api_client.utils import (get_area_uuid_from_area_name_and_collaboration_id,
                                  get_sim_id_and_domain_names)


class TestAggregator(Aggregator):
    """Aggregator used for receiving events from CN"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_pv_energy_forecast_Wh = 0
        self.current_load_energy_forecast_Wh = 0

    def on_market_cycle(self, market_info):
        """Execute the following code when a new market was started."""
        # set energy forecasts for the next market for load and pv
        pv.set_energy_forecast(self.current_pv_energy_forecast_Wh)
        load.set_energy_forecast(self.current_load_energy_forecast_Wh)
        # increase energy forecasts on each market slot
        self.current_pv_energy_forecast_Wh += 1
        self.current_load_energy_forecast_Wh += 1

    def on_tick(self, tick_info):
        """Execute the following code when a new tick was started."""
        logging.debug(f"Progress information on the device: {tick_info}")

    def on_trade(self, trade_info):
        """Execute the following code when a trade happened in the current market."""
        logging.debug(f"Trade info: {trade_info}")


simulation_id, domain_name, websockets_domain_name = get_sim_id_and_domain_names()

load_uuid = get_area_uuid_from_area_name_and_collaboration_id(simulation_id, "Load", domain_name)
load = RestDeviceClient(load_uuid)

pv_uuid = get_area_uuid_from_area_name_and_collaboration_id(simulation_id, "PV", domain_name)
pv = RestDeviceClient(pv_uuid)

aggregator = TestAggregator(aggregator_name="test_aggregator")

while True:
    sleep(0.5)
