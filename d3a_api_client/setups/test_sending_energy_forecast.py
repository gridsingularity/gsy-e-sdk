import logging
from time import sleep

from d3a_interface.constants_limits import DATE_TIME_FORMAT
from pendulum import from_format

from d3a_api_client.aggregator import Aggregator
from d3a_api_client.rest_device import RestDeviceClient
from d3a_api_client.utils import (get_area_uuid_from_area_name_and_collaboration_id,
                                  get_sim_id_and_domain_names)


class TestAggregator(Aggregator):
    """Aggregator used for receiving events from CN"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.future_pv_energy_forecast_kWh = 0
        self.current_load_energy_forecast_kWh = 0
        self.current_pv_energy_measurement_kWh = 0
        self.current_load_energy_measurement_kWh = 0

    def on_market_cycle(self, market_info):
        """Execute the following code when a new market was started."""
        # set energy forecasts for the next market for load and pv
        next_market_slot_str = (from_format(market_info["market_slot"], DATE_TIME_FORMAT).
                                add(minutes=15).format(DATE_TIME_FORMAT))
        load.set_energy_forecast({next_market_slot_str: self.future_pv_energy_forecast_kWh},
                                 do_not_wait=True)
        pv.set_energy_forecast({next_market_slot_str: self.future_pv_energy_forecast_kWh},
                               do_not_wait=True)
        # increase energy forecasts on each market slot
        self.future_pv_energy_forecast_kWh += 1
        self.current_load_energy_forecast_kWh += 1

        # set energy forecasts for the next market for load and pv
        pv.set_energy_measurement(
            {market_info["market_slot"]: self.current_pv_energy_measurement_kWh},
            do_not_wait=True)
        load.set_energy_measurement(
            {market_info["market_slot"]: self.current_load_energy_measurement_kWh},
            do_not_wait=True)
        # increase energy forecasts on each market slot
        self.current_pv_energy_measurement_kWh += 1
        self.current_load_energy_measurement_kWh += 1

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

pv.select_aggregator(aggregator.aggregator_uuid)
load.select_aggregator(aggregator.aggregator_uuid)

while True:
    sleep(0.5)
