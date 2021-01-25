from d3a_api_client.enums import Commands, command_enum_to_command_name


class ClientCommandBuffer:

    def __init__(self, ):
        self._commands_buffer = []

    def offer_energy(self, area_uuid, energy, price):
        area_uuid = area_uuid
        callback_args = {energy, price}
        action = Commands.OFFER
        self.execute(area_uuid, action, callback_args)

    def offer_energy_rate(self, area_uuid, energy, rate):
        area_uuid = area_uuid
        callback_args = {energy, rate}
        action = Commands.OFFER
        self.execute(area_uuid, action, callback_args)

    def bid_energy(self, area_uuid, energy, rate):
        area_uuid = area_uuid
        callback_args = {energy, rate}
        action = Commands.OFFER
        self.execute(area_uuid, action, callback_args)

    def delete_offer(self, area_uuid, offer_id):
        area_uuid = area_uuid
        callback_args = {"offer_id": offer_id}
        action = Commands.DELETE_OFFER
        self.execute(area_uuid, action, callback_args)

    def delete_bid(self, area_uuid, bid_id):
        area_uuid = area_uuid
        callback_args = {"bid_id": bid_id}
        action = Commands.DELETE_OFFER
        self.execute(area_uuid, action, callback_args)

    def list_offers(self, area_uuid):
        area_uuid = area_uuid
        action = Commands.LIST_OFFERS
        callback_args = {}
        self.execute(area_uuid, action, callback_args)

    def list_bids(self, area_uuid):
        area_uuid = area_uuid
        action = Commands.LIST_BIDS
        callback_args = {}
        self.execute(area_uuid, action, callback_args)

    def device_info(self, area_uuid):
        area_uuid = area_uuid
        action = Commands.DEVICE_INFO
        callback_args = {}
        self.execute(area_uuid, action, callback_args)

    def execute(self, area_uuid, action, callback_args):
        if area_uuid and action:
            self._commands_buffer.append(
                {area_uuid: {"type": command_enum_to_command_name(action), **callback_args}})

    def clear(self):
        self._commands_buffer.clear()

    def execute_batch(self):
        batch_command_dict = {}
        for command_dict in self._commands_buffer:
            area_uuid = command_dict.keys()[0]
            if area_uuid not in batch_command_dict.keys():
                batch_command_dict[area_uuid] = []
            batch_command_dict[area_uuid].append(command_dict[area_uuid])
        return batch_command_dict
