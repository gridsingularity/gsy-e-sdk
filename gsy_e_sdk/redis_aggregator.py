import json
import logging
import uuid
from concurrent.futures.thread import ThreadPoolExecutor
from copy import copy
from threading import Lock
from typing import Optional, Dict, List

from gsy_framework.client_connections.utils import (
    log_market_progression, get_slot_completion_percentage_int_from_message)
from gsy_framework.utils import wait_until_timeout_blocking, execute_function_util
from redis import Redis

from gsy_e_sdk.commands import ClientCommandBuffer
from gsy_e_sdk.constants import (
    MAX_WORKER_THREADS, MIN_SLOT_COMPLETION_TICK_TRIGGER_PERCENTAGE, LOCAL_REDIS_URL)
from gsy_e_sdk.grid_fee_calculation import GridFeeCalculation
from gsy_e_sdk.utils import (
    get_uuid_from_area_name_in_tree_dict, buffer_grid_tree_info,
    create_area_name_uuid_mapping_from_tree_info,
    get_name_from_area_name_uuid_mapping, log_trade_info,
    log_bid_offer_confirmation, log_deleted_bid_offer_confirmation)


class RedisAggregatorAPIException(Exception):
    """Exception that is raised in the RedisAggregator."""


class RedisAggregator:
    """Handle aggregator connection via redis to local running simulation."""

    # pylint: disable = too-many-instance-attributes
    def __init__(self, aggregator_name, accept_all_devices=True,
                 redis_url=LOCAL_REDIS_URL):

        self.grid_fee_calculation = GridFeeCalculation()
        self.redis_db = Redis.from_url(redis_url)
        self.pubsub = self.redis_db.pubsub()
        self.aggregator_name = aggregator_name
        self.aggregator_uuid = None
        self.accept_all_devices = accept_all_devices
        self._transaction_id_buffer = []
        self._transaction_id_response_buffer = {}
        self.device_uuid_list = []
        self._client_command_buffer = ClientCommandBuffer()

        self._connect_and_subscribe()

        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)
        self.lock = Lock()
        self.latest_grid_tree = {}
        self.latest_grid_tree_flat = {}
        self.area_name_uuid_mapping = {}

    def _connect_and_subscribe(self) -> None:
        # order matters here, first connect to the simulation,
        # then subscribe to all other channels that contain the aggregator_uuid
        self._subscribe_to_aggregator_response_and_start_redis_thread()
        self._connect_to_simulation()
        self._subscribe_to_response_channels()

    def _subscribe_to_aggregator_response_and_start_redis_thread(self) -> None:
        channel_dict = {"aggregator_response": self._aggregator_response_callback}
        self.pubsub.psubscribe(**channel_dict)
        self.pubsub.run_in_thread(daemon=True)

    def _connect_to_simulation(self, is_blocking: bool = True) -> None:
        if self.aggregator_uuid is None:
            aggr_id = self._create_aggregator(is_blocking=is_blocking)
            self.aggregator_uuid = aggr_id

    def _subscribe_to_response_channels(self) -> None:
        channel_dict = {f"external-aggregator/*/{self.aggregator_uuid}/events/all":
                        self._events_callback_dict,
                        f"external-aggregator/*/{self.aggregator_uuid}/response/batch_commands":
                        self._batch_response,
                        }
        self.pubsub.psubscribe(**channel_dict)

    # pylint: disable = logging-too-many-args
    def _batch_response(self, message: Dict) -> None:
        logging.debug("AGGREGATORS_BATCH_RESPONSE:: %s", message)
        data = json.loads(message["data"])
        if self.aggregator_uuid != data["aggregator_uuid"]:
            return
        with self.lock:
            self._transaction_id_buffer.pop(
                self._transaction_id_buffer.index(data["transaction_id"]))
            self._transaction_id_response_buffer[data["transaction_id"]] = data

        for asset_uuid, responses in data["responses"].items():
            for command_response in responses:
                log_bid_offer_confirmation(command_response)
                log_deleted_bid_offer_confirmation(
                    command_response,
                    asset_name=get_name_from_area_name_uuid_mapping(
                        self.area_name_uuid_mapping, asset_uuid))
        self.on_event_or_response(data)

    def _aggregator_response_callback(self, message: Dict) -> None:
        data = json.loads(message["data"])

        if data["transaction_id"] in self._transaction_id_buffer:
            self._transaction_id_buffer.remove(data["transaction_id"])
        if data["status"] == "SELECTED":
            self._selected_by_device(data)
        if data["status"] == "UNSELECTED":
            self._unselected_by_device(data)

    def _events_callback_dict(self, message: Dict) -> None:
        payload = json.loads(message["data"])
        if payload.get("event") == "market":
            self._on_market_cycle(payload)
        elif payload.get("event") == "tick":
            self._on_tick(payload)
        elif payload.get("event") == "trade":
            self._on_trade(payload)
        elif payload.get("event") == "finish":
            self._on_finish(payload)

        self._on_event_or_response(payload)

    def _is_transaction_response_received(self, transaction_id: str) -> bool:
        return transaction_id not in self._transaction_id_buffer

    def _create_aggregator(self, is_blocking: bool = True) -> Optional[str]:
        logging.info("Trying to create aggregator %s", self.aggregator_name)

        transaction_id = str(uuid.uuid4())
        data = {"name": self.aggregator_name, "type": "CREATE", "transaction_id": transaction_id}
        # IMPORTANT: Order matters in the following two steps because redis could be faster
        # than the appending of the transaction_id to the buffer:
        self._transaction_id_buffer.append(transaction_id)
        self.redis_db.publish("aggregator", json.dumps(data))

        if is_blocking:
            try:
                wait_until_timeout_blocking(
                    lambda: self._is_transaction_response_received(transaction_id)
                )
                return transaction_id
            except AssertionError as ex:
                raise RedisAggregatorAPIException("API registration process timed out.") from ex
        return None

    def delete_aggregator(self, is_blocking: bool = True) -> Optional[str]:
        """Delete aggregator."""
        logging.info("Trying to delete aggregator %s", self.aggregator_name)

        transaction_id = str(uuid.uuid4())
        data = {"name": self.aggregator_name,
                "aggregator_uuid": self.aggregator_uuid,
                "type": "DELETE",
                "transaction_id": transaction_id}
        self._transaction_id_buffer.append(transaction_id)
        self.redis_db.publish("aggregator", json.dumps(data))

        if is_blocking:
            try:
                wait_until_timeout_blocking(
                    lambda: self._is_transaction_response_received(transaction_id)
                )
                return transaction_id
            except AssertionError as ex:
                raise RedisAggregatorAPIException("API has timed out.") from ex
        return None

    def _selected_by_device(self, message: Dict) -> None:
        if self.accept_all_devices:
            self.device_uuid_list.append(message["device_uuid"])

    def _unselected_by_device(self, message: Dict) -> None:
        device_uuid = message["device_uuid"]
        if device_uuid in self.device_uuid_list:
            self.device_uuid_list.remove(device_uuid)

    def _all_uuids_in_selected_device_uuid_list(self, uuid_list: List) -> bool:
        for device_uuid in uuid_list:
            if device_uuid not in self.device_uuid_list:
                logging.error(
                    "%s not in list of selected device uuids %s",
                    device_uuid, self.device_uuid_list)
                raise Exception(f"{device_uuid} not in list of selected device uuids")
        return True

    @property
    def add_to_batch_commands(self) -> ClientCommandBuffer:
        """
        A property which is meant to be accessed prefixed to a chained function from the
        ClientCommandBuffer class
        This command will be added to the batch commands buffer
        """
        return self._client_command_buffer

    @property
    def commands_buffer_length(self) -> int:
        """
        Returns the length of the batch commands buffer
        """
        return self._client_command_buffer.buffer_length

    def execute_batch_commands(self, is_blocking: bool = True) -> Optional[str]:
        """Send all buffered batch commands to the simulation."""
        if not self.commands_buffer_length:
            return None
        batch_command_dict = self._client_command_buffer.execute_batch()
        self._client_command_buffer.clear()
        self._all_uuids_in_selected_device_uuid_list(batch_command_dict.keys())
        transaction_id = str(uuid.uuid4())
        batched_command = {"type": "BATCHED", "transaction_id": transaction_id,
                           "aggregator_uuid": self.aggregator_uuid,
                           "batch_commands": batch_command_dict}
        batch_channel = f"external//aggregator/{self.aggregator_uuid}/batch_commands"
        # IMPORTANT: Order matters in the following two steps because redis could be faster
        # than the appending of the transaction_id to the buffer:
        self._transaction_id_buffer.append(transaction_id)
        self.redis_db.publish(batch_channel, json.dumps(batched_command))

        if is_blocking:
            try:
                wait_until_timeout_blocking(
                    lambda: self._is_transaction_response_received(transaction_id)
                )
                return self._transaction_id_response_buffer.get(transaction_id, None)
            except AssertionError as ex:
                raise RedisAggregatorAPIException("Sending batch commands timed out.") from ex
        return None

    def _on_event_or_response(self, message: Dict) -> None:
        log_msg = copy(message)
        log_msg.pop("grid_tree", None)
        logging.debug("A new message was received. Message information: %s", log_msg)
        log_market_progression(message)
        self.executor.submit(execute_function_util,
                             function=lambda: self.on_event_or_response(message),
                             function_name="on_event_or_response")

    def calculate_grid_fee(self, start_market_or_device_name: str,
                           target_market_or_device_name: Optional[str] = None,
                           fee_type: str = "current_market_fee") -> Optional[float]:
        """Calculate accumulated grid_fee of path between start_market_or_device_name
        and target_market_or_device_name"""
        return self.grid_fee_calculation.calculate_grid_fee(start_market_or_device_name,
                                                            target_market_or_device_name, fee_type)

    def get_uuid_from_area_name(self, name: str) -> str:
        """Return area uuid from area name."""
        return get_uuid_from_area_name_in_tree_dict(self.area_name_uuid_mapping, name)

    @buffer_grid_tree_info
    def _on_market_cycle(self, message: Dict) -> None:
        self.area_name_uuid_mapping = \
            create_area_name_uuid_mapping_from_tree_info(self.latest_grid_tree_flat)
        self.grid_fee_calculation.handle_grid_stats(self.latest_grid_tree)
        self.executor.submit(
            execute_function_util,
            function=lambda: self.on_market_slot(message),
            function_name="on_market_slot")

    @buffer_grid_tree_info
    def _on_tick(self, message: Dict) -> None:
        slot_completion_int = get_slot_completion_percentage_int_from_message(message)
        if slot_completion_int is not None and slot_completion_int < \
                MIN_SLOT_COMPLETION_TICK_TRIGGER_PERCENTAGE:
            return
        self.executor.submit(execute_function_util, function=lambda: self.on_tick(message),
                             function_name="on_tick")

    @buffer_grid_tree_info
    def _on_trade(self, message: Dict) -> None:
        for individual_trade in message["trade_list"]:
            log_trade_info(individual_trade)
        self.executor.submit(execute_function_util, function=lambda: self.on_trade(message),
                             function_name="on_trade")

    def _on_finish(self, message: Dict) -> None:
        self.executor.submit(execute_function_util, function=lambda: self.on_finish(message),
                             function_name="on_finish")

    def on_market_cycle(self, market_info):
        """(DEPRECATED) Perform actions that should be triggered on market_cycle event.

        This method was deprecated in favor of the new `on_market_slot`.
        """

    def on_market_slot(self, market_info):
        """Perform actions that should be triggered on market event."""
        self.on_market_cycle(market_info)

    def on_tick(self, tick_info):
        """Perform actions that should be triggered on tick event."""

    def on_trade(self, trade_info):
        """Perform actions that should be triggered on on_trade event."""

    def on_finish(self, finish_info):
        """Perform actions that should be triggered on on_finish event."""

    def on_event_or_response(self, message):
        """Perform actions that should be triggered on every event."""
