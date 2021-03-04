import logging
from copy import copy

from d3a_interface.utils import key_in_dict_and_not_none


class GridFeeCalculation:

    def __init__(self):
        self.latest_grid_stats_tree = {}
        self.paths_to_root_mapping = {}
        self.market_area_uuid_grid_fee_mapping = {"last_market_fee": {},
                                                  "current_market_fee": {}}

    def handle_grid_stats(self, latest_grid_tree):
        self.latest_grid_stats_tree = latest_grid_tree
        self._get_grid_fee_area_mapping_and_paths_from_grid_stats_dict(self.latest_grid_stats_tree, [])

    def _get_grid_fee_area_mapping_and_paths_from_grid_stats_dict(self, indict, parent_path):
        for child_uuid, child_stats in indict.items():
            sub_path = parent_path + [child_uuid]
            self.paths_to_root_mapping[child_uuid] = parent_path
            for fee_type in ["last_market_fee", "current_market_fee"]:
                if fee_type in child_stats:
                    self.market_area_uuid_grid_fee_mapping[fee_type][child_uuid] = child_stats[fee_type]
            if "children" in child_stats:
                self._get_grid_fee_area_mapping_and_paths_from_grid_stats_dict(child_stats["children"], sub_path)

    @staticmethod
    def _strip_away_intersection_from_list(in_list, intersection):
        return list(set(in_list) ^ set(intersection))

    @staticmethod
    def _find_lowest_intersection_market(in_list, intersection):
        last_li = in_list[0]
        for li in in_list:
            if li not in intersection:
                return last_li
            last_li = li
        return last_li

    def calculate_grid_fee(self, start_market_or_device_uuid: str,
                           target_market_or_device_uuid: str = None,
                           fee_type: str = "current_market_fee"):
        """
        Calculates grid fees along path between two assets or markets in the grid
        """
        if not self.latest_grid_stats_tree:
            logging.info("Grid fees can not be calculated because there were no grid_stats sent yet.")
            return None

        if target_market_or_device_uuid is None:
            # only return the grid_fee of the connected market
            if start_market_or_device_uuid not in self.market_area_uuid_grid_fee_mapping[fee_type]:
                # if the target_market_or_device is a device, return the grid_fee of the connected market
                return self.market_area_uuid_grid_fee_mapping[fee_type][
                    self.paths_to_root_mapping[start_market_or_device_uuid][-1]]
            else:
                # if the target_market_or_device is a market, return the grid_fee directly
                return self.market_area_uuid_grid_fee_mapping[fee_type][start_market_or_device_uuid]

        path_start_market = self.paths_to_root_mapping[start_market_or_device_uuid]
        path_target_market = self.paths_to_root_mapping[target_market_or_device_uuid]

        intersection_markets = list(set(path_start_market).intersection(path_target_market))

        path_start_market_stripped = \
            self._strip_away_intersection_from_list(path_start_market, intersection_markets)
        path_target_market_stripped = \
            self._strip_away_intersection_from_list(path_target_market, intersection_markets)

        if path_start_market_stripped == [target_market_or_device_uuid]:
            # case when start_market child of target_market
            all_markets_along_path = path_start_market_stripped
        elif path_target_market_stripped == [start_market_or_device_uuid]:
            # case when target_market child of start_market
            all_markets_along_path = path_target_market_stripped
        else:
            lowest_intersection_market = \
                self._find_lowest_intersection_market(path_start_market, intersection_markets)

            all_markets_along_path = set([lowest_intersection_market] +
                                         path_start_market_stripped + path_target_market_stripped +
                                         [start_market_or_device_uuid] + [target_market_or_device_uuid])

        total_grid_fees = 0
        for ma in all_markets_along_path:
            if key_in_dict_and_not_none(self.market_area_uuid_grid_fee_mapping[fee_type], ma):
                total_grid_fees += self.market_area_uuid_grid_fee_mapping[fee_type][ma]

        return total_grid_fees
