# pylint: disable=invalid-name

import logging
from typing import Dict, List

from tabulate import tabulate

from gsy_e_sdk.enums import Commands, command_enum_to_command_name


# pylint: disable=too-many-arguments
class ClientCommandBuffer:
    """Buffer used to keep in memory the batch commands until they're submitted to the server."""
    def __init__(self, ):
        self._commands_buffer = []

    @property
    def buffer_length(self):
        """Return the number of commands added to the buffer up to this moment."""
        return len(self._commands_buffer)

    def offer_energy(
            self, asset_uuid: str, energy: float, price: float, replace_existing: bool = True,
            attributes: Dict = None, requirements: List[Dict] = None, time_slot: str = None):
        """Add a command to issue an offer with the given price and additional parameters."""
        return self._add_to_buffer(
            asset_uuid,
            Commands.OFFER,
            {"energy": energy, "price": price, "replace_existing": replace_existing,
             "attributes": attributes, "requirements": requirements, "time_slot": time_slot})

    def offer_energy_rate(
            self, asset_uuid: str, energy: float, rate: float, replace_existing: bool = True,
            attributes: Dict = None, requirements: List[Dict] = None, time_slot: str = None):
        """Add a command to issue an offer with the given energy rate and additional parameters."""
        return self._add_to_buffer(
            asset_uuid,
            Commands.OFFER,
            {"energy": energy, "price": rate * energy, "replace_existing": replace_existing,
             "attributes": attributes, "requirements": requirements, "time_slot": time_slot})

    # pylint: disable=unused-argument
    # pylint: disable=no-self-use
    def update_offer(self, *args, **kwargs):
        """Add a command to update an offer."""
        logging.warning("update_offer is deprecated,"
                        " use offer_energy with replace_existing=True instead.")

    def bid_energy(
            self, asset_uuid: str, energy: float, price: float, replace_existing: bool = True,
            attributes: Dict = None, requirements: List[Dict] = None, time_slot: str = None):
        """Add a command to issue a bid with the given price and additional parameters."""
        return self._add_to_buffer(
            asset_uuid,
            Commands.BID,
            {"energy": energy, "price": price, "replace_existing": replace_existing,
             "attributes": attributes, "requirements": requirements, "time_slot": time_slot})

    def bid_energy_rate(
            self, asset_uuid: str, energy: float, rate: float, replace_existing: bool = True,
            attributes: Dict = None, requirements: List[Dict] = None, time_slot: str = None):
        """Add a command to issue a bid with the given energy rate and additional parameters."""
        return self._add_to_buffer(
            asset_uuid,
            Commands.BID,
            {"energy": energy, "price": rate * energy, "replace_existing": replace_existing,
             "attributes": attributes, "requirements": requirements, "time_slot": time_slot})

    def update_bid(self, *args, **kwargs):
        """Add a command to update a bid."""
        logging.warning("update_bid is deprecated,"
                        " use bid_energy with replace_existing=True instead.")

    def delete_offer(self, asset_uuid, offer_id, time_slot: str = None):
        """
        Add a command to delete a specific offer from a specific asset in the given time slot.
        """
        return self._add_to_buffer(asset_uuid, Commands.DELETE_OFFER,
                                   {"offer_id": offer_id, "time_slot": time_slot})

    def delete_bid(self, asset_uuid, bid_id, time_slot: str = None):
        """Add a command to delete a specific bid from a specific asset, in the given time slot."""
        return self._add_to_buffer(asset_uuid, Commands.DELETE_BID,
                                   {"bid_id": bid_id, "time_slot": time_slot})

    def list_offers(self, area_uuid, time_slot: str = None):
        """Add a command to list offers made on the specified area in the given time slot."""
        return self._add_to_buffer(area_uuid, Commands.LIST_OFFERS, {"time_slot": time_slot})

    def list_bids(self, area_uuid, time_slot: str = None):
        """Add a command to list bids made on the specified area in the given time slot."""
        return self._add_to_buffer(area_uuid, Commands.LIST_BIDS, {"time_slot": time_slot})

    def device_info(self, area_uuid):
        """Retrieve information about the asset identified by the given UUID.

        Important: this method is deprecated. Please use `asset_info` instead.
        """
        logging.warning("device_info is deprecated. Please use asset_info instead.")
        return self.asset_info(area_uuid)

    def asset_info(self, asset_uuid):
        """Retrieve information about the asset identified by the given UUID."""
        return self._add_to_buffer(asset_uuid, Commands.ASSET_INFO, {})

    def last_market_dso_stats(self, area_uuid):
        """Add a command to retrieve the statistics of a Distribution System Operator."""
        return self._add_to_buffer(area_uuid, Commands.DSO_MARKET_STATS, {"data": {}})

    def set_energy_forecast(self, asset_uuid, energy_forecast_kWh: Dict):
        """Add a command to set the energy forecast of the given asset."""
        return self._add_to_buffer(asset_uuid, Commands.FORECAST,
                                   {"energy_forecast": energy_forecast_kWh})

    def set_energy_measurement(self, asset_uuid, energy_measurement_kWh: Dict):
        """Add a command to set the actual energy measurement of the given asset."""
        return self._add_to_buffer(asset_uuid, Commands.MEASUREMENT,
                                   {"energy_measurement": energy_measurement_kWh})

    def change_grid_fees_percent(self, area_uuid, fee_percent):
        """Add a command to change the grid fees of the given market using a percentual value."""
        return self._add_to_buffer(
            area_uuid,
            Commands.GRID_FEES,
            {"data": {"fee_percent": fee_percent}})

    def grid_fees(self, area_uuid, fee_cents_kwh):
        """Add a command to change the grid fees of the given market using a constant value."""
        return self._add_to_buffer(
            area_uuid,
            Commands.GRID_FEES,
            {"data": {"fee_const": fee_cents_kwh}})

    def _add_to_buffer(self, area_uuid, action, args):
        if area_uuid and action:
            self._commands_buffer.append(
                {area_uuid: {"type": command_enum_to_command_name(action)
                             if isinstance(action, Commands) else action, **args, **args}})
            logging.debug("Added Command to buffer, updated buffer: ")
            self._log_all_commands()
        return self

    def clear(self):
        """Remove all commands that were previously added to the buffer."""
        self._commands_buffer.clear()

    def _log_all_commands(self):
        """Log all commands that were previously added to the buffer."""
        table_headers = ["Area UUID", "Command Type", "Arguments"]
        table_data = []
        for command_dict in self._commands_buffer:
            area_uuid = list(command_dict.keys())[0]
            command_type = command_dict[area_uuid]["type"]
            command_args = str(command_dict[area_uuid])
            table_data.append([area_uuid, command_type, command_args])
        logging.debug(
            "\n\n%s\n\n", tabulate(table_data, headers=table_headers, tablefmt="fancy_grid"))

    def execute_batch(self):
        """Send to the exchange all the commands that were previously added to the buffer."""
        batch_command_dict = {}
        for command_dict in self._commands_buffer:
            area_uuid = list(command_dict.keys())[0]
            if area_uuid not in batch_command_dict.keys():
                batch_command_dict[area_uuid] = []
            batch_command_dict[area_uuid].append(command_dict[area_uuid])
        return batch_command_dict
