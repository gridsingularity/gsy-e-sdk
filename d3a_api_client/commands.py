from d3a_api_client.enums import Commands, command_enum_to_command_name


class ClientCommandBuffer:

    def __init__(self, ):
        self._commands_buffer = []

    def offer_energy(self, area_uuid, energy, price):
        self._add_to_buffer(area_uuid, Commands.OFFER, {"energy": energy, "price": price})

    def offer_energy_rate(self, area_uuid, energy, rate):
        self._add_to_buffer(area_uuid, Commands.OFFER, {"energy": energy, "rate": rate})

    def bid_energy(self, area_uuid, energy, price):
        self._add_to_buffer(area_uuid, Commands.BID, {"energy": energy, "price": price})

    def bid_energy_rate(self, area_uuid, energy, rate):
        self._add_to_buffer(area_uuid, Commands.BID, {"energy": energy, "rate": rate})

    def delete_offer(self, area_uuid, offer_id):
        self._add_to_buffer(area_uuid, Commands.DELETE_OFFER, {"offer_id": offer_id})

    def delete_bid(self, area_uuid, bid_id):
        self._add_to_buffer(area_uuid, Commands.DELETE_BID, {"bid_id": bid_id})

    def list_offers(self, area_uuid):
        self._add_to_buffer(area_uuid, Commands.LIST_OFFERS, {})

    def list_bids(self, area_uuid):
        self._add_to_buffer(area_uuid, Commands.LIST_BIDS, {})

    def device_info(self, area_uuid):
        self._add_to_buffer(area_uuid, Commands.DEVICE_INFO, {})

    def _add_to_buffer(self, area_uuid, action, args):
        if area_uuid and action:
            self._commands_buffer.append(
                {area_uuid: {"type": command_enum_to_command_name(action), **args}})

    def clear(self):
        self._commands_buffer.clear()

    def execute_batch(self):
        batch_command_dict = {}
        for command_dict in self._commands_buffer:
            area_uuid = list(command_dict.keys())[0]
            if area_uuid not in batch_command_dict.keys():
                batch_command_dict[area_uuid] = []
            batch_command_dict[area_uuid].append(command_dict[area_uuid])
        return batch_command_dict
