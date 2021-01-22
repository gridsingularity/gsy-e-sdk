from abc import ABC, abstractmethod

from d3a_api_client.enums import Commands, command_enum_to_command_name


class Command(ABC):
    @abstractmethod
    def execute(self):
        pass


class ClientCommand(Command):

    def __init__(self, ):
        super(ClientCommand, self).__init__()
        self.area_uuid = None
        self.action = None
        self.callback = None
        self.callback_args = {}

    def offer_energy(self, area_uuid, energy, price):
        self.area_uuid = area_uuid
        self.callback_args = {energy, price}
        self.action = Commands.OFFER

    def offer_energy_rate(self, area_uuid, energy, rate):
        self.area_uuid = area_uuid
        self.callback_args = {energy, rate}
        self.action = Commands.OFFER

    def bid_energy(self, area_uuid, energy, rate):
        self.area_uuid = area_uuid
        self.callback_args = {energy, rate}
        self.action = Commands.OFFER

    def delete_offer(self, area_uuid, offer_id):
        self.area_uuid = area_uuid
        self.callback_args = {"offer_id": offer_id}
        self.action = Commands.DELETE_OFFER

    def delete_bid(self, area_uuid, bid_id):
        self.area_uuid = area_uuid
        self.callback_args = {"bid_id": bid_id}
        self.action = Commands.DELETE_OFFER

    def list_offers(self, area_uuid):
        self.area_uuid = area_uuid
        self.action = Commands.LIST_OFFERS

    def list_bids(self, area_uuid):
        self.area_uuid = area_uuid
        self.action = Commands.LIST_BIDS

    def device_info(self, area_uuid):
        self.area_uuid = area_uuid
        self.action = Commands.DEVICE_INFO

    def execute(self) -> dict:
        if self.area_uuid and self.action:
            return {self.area_uuid: {"type": command_enum_to_command_name(self.action), **self.callback_args}}

    @staticmethod
    def execute_batch(commands:list):
        batch_command_dict = {}
        for command in commands:
            command_dict = command.execute()
            area_uuid = command_dict.keys()[0]
            if area_uuid not in batch_command_dict.keys():
                batch_command_dict[area_uuid] = []
            batch_command_dict[area_uuid].append(command_dict[area_uuid])
        return batch_command_dict
