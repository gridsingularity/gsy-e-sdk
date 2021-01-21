from abc import ABC, abstractmethod

from d3a_api_client.enums import Commands, command_enum_to_command_name


class Command(ABC):
    @abstractmethod
    def execute(self):
        pass


class ClientCommand:

    def __init__(self, area_uuid):
        super(ClientCommand, self).__init__()
        self.area_uuid = area_uuid
        self.action = None
        self.callback = None
        self.callback_args = {}

    def offer_energy(self, energy, price):
        self.callback_args = {energy, price}
        self.action = Commands.OFFER

    def offer_energy_rate(self, energy, rate):
        self.callback_args = {energy, rate}
        self.action = Commands.OFFER

    def bid_energy(self, energy, rate):
        self.callback_args = {energy, rate}
        self.action = Commands.OFFER

    def delete_offer(self, offer_id):
        self.callback_args = {"offer_id": offer_id}
        self.action = Commands.DELETE_OFFER

    def delete_bid(self, bid_id):
        self.callback_args = {"bid_id": bid_id}
        self.action = Commands.DELETE_OFFER

    def list_offers(self):
        self.action = Commands.LIST_OFFERS

    def list_bids(self):
        self.action = Commands.LIST_BIDS

    def device_info(self):
        self.action = Commands.DEVICE_INFO

    def execute(self) -> dict:
        if self.area_uuid:
            return {self.area_uuid: {"type": command_enum_to_command_name(self.action), **self.callback_args}}


class ClientCommandList(Command, list):
    def __init__(self):
        super(ClientCommandList, self).__init__()

    def execute(self) -> dict:
        batch_command_dict = {}
        for command in self:
            command_dict = command.execute()
            area_uuid = command_dict.keys()[0]
            if area_uuid not in batch_command_dict.keys():
                batch_command_dict[area_uuid] = []
            batch_command_dict[area_uuid].append(command_dict[area_uuid])
        self.clear()
        return batch_command_dict
