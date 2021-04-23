import json
import logging
import uuid
from concurrent.futures.thread import ThreadPoolExecutor
from copy import copy
from threading import Lock

from d3a_interface.utils import wait_until_timeout_blocking
from redis import StrictRedis

from d3a_api_client.commands import ClientCommandBuffer
from d3a_api_client.constants import (
    MAX_WORKER_THREADS, MIN_SLOT_COMPLETION_TICK_TRIGGER_PERCENTAGE)
from d3a_api_client.grid_fee_calculation import GridFeeCalculation
from d3a_api_client.utils import (
    execute_function_util, log_market_progression, log_trade_info,
    log_bid_offer_confirmation, log_deleted_bid_offer_confirmation)
from d3a_api_client.utils import (
    get_uuid_from_area_name_in_tree_dict, buffer_grid_tree_info,
    create_area_name_uuid_mapping_from_tree_info, get_slot_completion_percentage_int_from_message)


class RedisAPIException(Exception):
    pass


class RedisAggregator:

    def __init__(self, aggregator_name, accept_all_devices=True,
                 redis_url='redis://localhost:6379'):

        self.grid_fee_calculation = GridFeeCalculation()
        self.redis_db = StrictRedis.from_url(redis_url)
        self.pubsub = self.redis_db.pubsub()
        self.aggregator_name = aggregator_name
        self.aggregator_uuid = None
        self.accept_all_devices = accept_all_devices
        self._transaction_id_buffer = []
        self._transaction_id_response_buffer = {}
        self.device_uuid_list = []
        self._subscribed_aggregator_response_cb = None
        self._client_command_buffer = ClientCommandBuffer()
        self._connect_to_simulation(is_blocking=True)
        self._subscribe_to_response_channels()
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)
        self.lock = Lock()
        self.latest_grid_tree = {}
        self.latest_grid_tree_flat = {}
        self.area_name_uuid_mapping = {}

    def _connect_to_simulation(self, is_blocking=True):
        if self.aggregator_uuid is None:
            aggr_id = self._create_aggregator(is_blocking=is_blocking)
            self.aggregator_uuid = aggr_id

    def _subscribe_to_response_channels(self):

        if b'aggregator_response' in self.pubsub.patterns:
            self._subscribed_aggregator_response_cb = self.pubsub.patterns[b'aggregator_response']

        event_channel = f'external-aggregator/*/{self.aggregator_uuid}/events/all'
        channel_dict = {event_channel: self._events_callback_dict,
                        "aggregator_response": self._aggregator_response_callback,
                        f"external-aggregator/*/{self.aggregator_uuid}/response/batch_commands":
                            self._batch_response,
                        }

        self.pubsub.psubscribe(**channel_dict)
        self.pubsub.run_in_thread(daemon=True)

    def _batch_response(self, message):
        logging.debug(f"AGGREGATORS_BATCH_RESPONSE:: {message}")
        data = json.loads(message['data'])
        if self.aggregator_uuid != data['aggregator_uuid']:
            return
        with self.lock:
            self._transaction_id_buffer.pop(self._transaction_id_buffer.index(data['transaction_id']))
            self._transaction_id_response_buffer[data['transaction_id']] = data

        def executor_function():
            self.on_batch_response(data['responses'])

        self.executor.submit(executor_function)
        for asset_uuid, responses in data["responses"].items():
            for command_response in responses:
                log_bid_offer_confirmation(command_response)
                log_deleted_bid_offer_confirmation(command_response)

    def _aggregator_response_callback(self, message):
        if self._subscribed_aggregator_response_cb is not None:
            self._subscribed_aggregator_response_cb(message)
        data = json.loads(message['data'])

        if data['transaction_id'] in self._transaction_id_buffer:
            self._transaction_id_buffer.pop(self._transaction_id_buffer.index(data['transaction_id']))
        if data['status'] == "SELECTED":
            self._selected_by_device(data)
        if data['status'] == "UNSELECTED":
            self._unselected_by_device(data)

    def _events_callback_dict(self, message):
        payload = json.loads(message['data'])
        if "event" in payload and payload['event'] == 'market':
            self._on_market_cycle(payload)
        elif "event" in payload and payload["event"] == "tick":
            self._on_tick(payload)
        elif "event" in payload and payload["event"] == "trade":
            self._on_trade(payload)
        elif "event" in payload and payload["event"] == "finish":
            self._on_finish(payload)

        self._on_event_or_response(payload)

    def _check_transaction_id_cached_out(self, transaction_id):
        return transaction_id in self._transaction_id_buffer

    def _create_aggregator(self, is_blocking=True):
        logging.info(f"Trying to create aggregator {self.aggregator_name}")

        transaction_id = str(uuid.uuid4())
        data = {"name": self.aggregator_name, "type": "CREATE", "transaction_id": transaction_id}
        self.redis_db.publish(f'aggregator', json.dumps(data))
        self._transaction_id_buffer.append(transaction_id)

        if is_blocking:
            try:
                wait_until_timeout_blocking(
                    lambda: self._check_transaction_id_cached_out(transaction_id)
                )
                return transaction_id
            except AssertionError:
                raise RedisAPIException(f'API registration process timed out.')

    def delete_aggregator(self, is_blocking=True):
        logging.info(f"Trying to delete aggregator {self.aggregator_name}")

        transaction_id = str(uuid.uuid4())
        data = {"name": self.aggregator_name,
                "aggregator_uuid": self.aggregator_uuid,
                "type": "DELETE",
                "transaction_id": transaction_id}
        self.redis_db.publish(f'aggregator', json.dumps(data))
        self._transaction_id_buffer.append(transaction_id)

        if is_blocking:
            try:
                wait_until_timeout_blocking(
                    lambda: self._check_transaction_id_cached_out(transaction_id)
                )
                return transaction_id
            except AssertionError:
                raise RedisAPIException(f'API has timed out.')

    def _selected_by_device(self, message):
        if self.accept_all_devices:
            self.device_uuid_list.append(message["device_uuid"])

    def _unselected_by_device(self, message):
        device_uuid = message["device_uuid"]
        if device_uuid in self.device_uuid_list:
            self.device_uuid_list.remove(device_uuid)

    def _all_uuids_in_selected_device_uuid_list(self, uuid_list):
        for device_uuid in uuid_list:
            if device_uuid not in self.device_uuid_list:
                logging.error(f"{device_uuid} not in list of selected device uuids {self.device_uuid_list}")
                raise Exception(f"{device_uuid} not in list of selected device uuids")
        return True

    @property
    def add_to_batch_commands(self):
        """
        A property which is meant to be accessed prefixed to a chained function from the ClientCommandBuffer class
        This command will be added to the batch commands buffer
        """
        return self._client_command_buffer

    @property
    def commands_buffer_length(self):
        """
        Returns the length of the batch commands buffer
        """
        return self._client_command_buffer.buffer_length

    def execute_batch_commands(self, is_blocking=True):
        if not self.commands_buffer_length:
            return
        batch_command_dict = self._client_command_buffer.execute_batch()
        self._client_command_buffer.clear()
        self._all_uuids_in_selected_device_uuid_list(batch_command_dict.keys())
        transaction_id = str(uuid.uuid4())
        batched_command = {"type": "BATCHED", "transaction_id": transaction_id,
                           "aggregator_uuid": self.aggregator_uuid,
                           "batch_commands": batch_command_dict}
        batch_channel = f'external//aggregator/{self.aggregator_uuid}/batch_commands'
        self.redis_db.publish(batch_channel, json.dumps(batched_command))
        self._transaction_id_buffer.append(transaction_id)
        if is_blocking:
            try:
                wait_until_timeout_blocking(
                    lambda: not self._check_transaction_id_cached_out(transaction_id)
                )
                return self._transaction_id_response_buffer.get(transaction_id, None)
            except AssertionError:
                raise RedisAPIException(f'API registration process timed out.')

    def _on_event_or_response(self, message):
        log_msg = copy(message)
        log_msg.pop("grid_tree", None)
        logging.debug(f"A new message was received. Message information: {log_msg}")
        log_market_progression(message)
        self.executor.submit(execute_function_util,
                             function=lambda: self.on_event_or_response(message),
                             function_name="on_event_or_response")

    def calculate_grid_fee(self, start_market_or_device_name: str,
                           target_market_or_device_name: str = None,
                           fee_type: str = "current_market_fee"):
        return self.grid_fee_calculation.calculate_grid_fee(start_market_or_device_name,
                                                            target_market_or_device_name, fee_type)

    def get_uuid_from_area_name(self, name):
        return get_uuid_from_area_name_in_tree_dict(self.area_name_uuid_mapping, name)

    @buffer_grid_tree_info
    def _on_market_cycle(self, message):
        self.area_name_uuid_mapping = \
            create_area_name_uuid_mapping_from_tree_info(self.latest_grid_tree_flat)
        self.grid_fee_calculation.handle_grid_stats(self.latest_grid_tree)
        self.executor.submit(execute_function_util, function=lambda: self.on_market_cycle(message),
                             function_name="on_market_cycle")

    @buffer_grid_tree_info
    def _on_tick(self, message):
        slot_completion_int = get_slot_completion_percentage_int_from_message(message)
        if slot_completion_int is not None and slot_completion_int < \
                MIN_SLOT_COMPLETION_TICK_TRIGGER_PERCENTAGE:
            return
        self.executor.submit(execute_function_util, function=lambda: self.on_tick(message),
                             function_name="on_tick")

    @buffer_grid_tree_info
    def _on_trade(self, message):
        # Aggregator message
        for individual_trade in message["trade_list"]:
            log_trade_info(individual_trade)
        self.executor.submit(execute_function_util, function=lambda: self.on_trade(message),
                             function_name="on_trade")

    def _on_finish(self, message):
        self.executor.submit(execute_function_util, function=lambda: self.on_finish(message),
                             function_name="on_finish")

    def on_market_cycle(self, market_info):
        pass

    def on_tick(self, tick_info):
        pass

    def on_trade(self, trade_info):
        pass

    def on_finish(self, finish_info):
        pass

    def on_batch_response(self, message):
        pass

    def on_event_or_response(self, message):
        pass
