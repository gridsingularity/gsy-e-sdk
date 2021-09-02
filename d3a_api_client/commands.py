import logging
from typing import Dict, List

from tabulate import tabulate

from d3a_api_client.enums import Commands, command_enum_to_command_name


class ClientCommandBuffer:

    def __init__(self, ):
        self._commands_buffer = []

    @property
    def buffer_length(self):
        return len(self._commands_buffer)

    def offer_energy(
            self, area_uuid: str, energy: float, price: float, replace_existing: bool = True,
            attributes: Dict = None, requirements: List[Dict] = None, timeslot: str = None):

        return self._add_to_buffer(
            area_uuid,
            Commands.OFFER,
            {"energy": energy, "price": price, "replace_existing": replace_existing,
             "attributes": attributes, "requirements": requirements, "timeslot": timeslot})

    def offer_energy_rate(
            self, area_uuid: str, energy: float, rate: float, replace_existing: bool = True,
            attributes: Dict = None, requirements: List[Dict] = None, timeslot: str = None):

        return self._add_to_buffer(
            area_uuid,
            Commands.OFFER,
            {"energy": energy, "price": rate * energy, "replace_existing": replace_existing,
             "attributes": attributes, "requirements": requirements, "timeslot": timeslot})

    def update_offer(self, *args, **kwargs):
        logging.warning("update_offer is deprecated,"
                        " use offer_energy with replace_existing=True instead.")

    def bid_energy(
            self, area_uuid: str, energy: float, price: float, replace_existing: bool = True,
            attributes: Dict = None, requirements: List[Dict] = None, timeslot: str = None):

        return self._add_to_buffer(
            area_uuid,
            Commands.BID,
            {"energy": energy, "price": price, "replace_existing": replace_existing,
             "attributes": attributes, "requirements": requirements, "timeslot": timeslot})

    def bid_energy_rate(
            self, area_uuid: str, energy: float, rate: float, replace_existing: bool = True,
            attributes: Dict = None, requirements: List[Dict] = None, timeslot: str = None):

        return self._add_to_buffer(
            area_uuid,
            Commands.BID,
            {"energy": energy, "price": rate * energy, "replace_existing": replace_existing,
             "attributes": attributes, "requirements": requirements, "timeslot": timeslot})

    def update_bid(self, *args, **kwargs):
        logging.warning("update_bid is deprecated,"
                        " use bid_energy with replace_existing=True instead.")

    def delete_offer(self, area_uuid, offer_id, timeslot: str = None):
        return self._add_to_buffer(area_uuid, Commands.DELETE_OFFER, {"offer_id": offer_id, "timeslot": timeslot})

    def delete_bid(self, area_uuid, bid_id, timeslot: str = None):
        return self._add_to_buffer(area_uuid, Commands.DELETE_BID, {"bid_id": bid_id, "timeslot": timeslot})

    def list_offers(self, area_uuid, timeslot: str = None):
        return self._add_to_buffer(area_uuid, Commands.LIST_OFFERS, {"timeslot": timeslot})

    def list_bids(self, area_uuid, timeslot: str = None):
        return self._add_to_buffer(area_uuid, Commands.LIST_BIDS, {"timeslot": timeslot})

    def device_info(self, area_uuid):
        return self._add_to_buffer(area_uuid, Commands.DEVICE_INFO, {})

    def last_market_dso_stats(self, area_uuid):
        return self._add_to_buffer(area_uuid, Commands.DSO_MARKET_STATS, {"data": {}})

    def set_energy_forecast(self, area_uuid, energy_forecast_kWh: Dict  ):
        return self._add_to_buffer(area_uuid, Commands.FORECAST, {"energy_forecast": energy_forecast_kWh})

    def set_energy_measurement(self, area_uuid, energy_measurement_kWh: Dict):
        return self._add_to_buffer(area_uuid, Commands.MEASUREMENT, {"energy_measurement": energy_measurement_kWh})

    def change_grid_fees_percent(self, area_uuid, fee_percent):
        return self._add_to_buffer(
            area_uuid,
            Commands.GRID_FEES,
            {"data": {"fee_percent": fee_percent}})

    def grid_fees(self, area_uuid, fee_cents_kwh):
        return self._add_to_buffer(
            area_uuid,
            Commands.GRID_FEES,
            {"data": {"fee_const": fee_cents_kwh}})

    def _add_to_buffer(self, area_uuid, action, args):
        if area_uuid and action:
            self._commands_buffer.append(
                {area_uuid: {"type": command_enum_to_command_name(action)
                             if type(action) == Commands else action, **args, **args}})
            logging.debug("Added Command to buffer, updated buffer: ")
            self.log_all_commands()
        return self

    def clear(self):
        self._commands_buffer.clear()

    def log_all_commands(self):
        table_headers = ["Area UUID", "Command Type", "Arguments"]
        table_data = []
        for command_dict in self._commands_buffer:
            area_uuid = list(command_dict.keys())[0]
            command_type = command_dict[area_uuid]["type"]
            command_args = str(command_dict[area_uuid])
            table_data.append([area_uuid, command_type, command_args])
        logging.debug(f"\n\n{tabulate(table_data, headers=table_headers, tablefmt='fancy_grid')}\n\n")

    def execute_batch(self):
        batch_command_dict = {}
        for command_dict in self._commands_buffer:
            area_uuid = list(command_dict.keys())[0]
            if area_uuid not in batch_command_dict.keys():
                batch_command_dict[area_uuid] = []
            batch_command_dict[area_uuid].append(command_dict[area_uuid])
        return batch_command_dict
