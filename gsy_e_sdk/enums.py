from enum import Enum


class Commands(Enum):
    OFFER = 1
    BID = 2
    DELETE_OFFER = 3
    DELETE_BID = 4
    LIST_OFFERS = 5
    LIST_BIDS = 6
    ASSET_INFO = 7
    GRID_FEES = 8
    DSO_MARKET_STATS = 9
    FORECAST = 10
    MEASUREMENT = 11


command_enum_to_command_name_dict = {
    Commands.OFFER: "offer",
    Commands.BID: "bid",
    Commands.DELETE_OFFER: "delete_offer",
    Commands.DELETE_BID: "delete_bid",
    Commands.LIST_OFFERS: "list_offers",
    Commands.LIST_BIDS: "list_bids",
    Commands.ASSET_INFO: "device_info",
    Commands.GRID_FEES: "grid_fees",
    Commands.DSO_MARKET_STATS: "dso_market_stats",
    Commands.FORECAST: "set_energy_forecast",
    Commands.MEASUREMENT: "set_energy_measurement"
}


def command_enum_to_command_name(command: Commands) -> str:
    return command_enum_to_command_name_dict[command]
