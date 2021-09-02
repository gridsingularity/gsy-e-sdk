"""
Copyright 2018 Grid Singularity
This file is part of D3A.
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.
You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
import json
from unittest.mock import MagicMock, patch
import uuid
import pytest
from redis import StrictRedis
import d3a_api_client
from d3a_api_client.redis_aggregator import RedisAggregator, RedisAPIException

d3a_api_client.redis_aggregator.StrictRedis = MagicMock(spec=StrictRedis)
AGGREGATOR_NAME = str(uuid.uuid4())
TRANSACTION_ID = str(uuid.uuid4())


# pylint: disable=protected-access
class TestRedisAggregator:
    """Testing the behaviour of redis aggregator without connecting to d3a."""

    @pytest.fixture()
    def redis_aggregator_auto_register(self):
        return RedisAggregator(AGGREGATOR_NAME)

    @staticmethod
    def test_connect_to_simulation(redis_aggregator_auto_register):
        redis_aggregator_auto_register._connect_to_simulation(is_blocking=True)
        aggr_id = redis_aggregator_auto_register._create_aggregator()
        aggregator_id = redis_aggregator_auto_register._transaction_id_buffer[1]
        assert aggr_id == aggregator_id

    @staticmethod
    @patch("d3a_api_client.redis_aggregator.StrictRedis.pubsub.psubscribe")
    def test_subscribe_to_response_channels(mock_pubsub_psubscribe,
                                            redis_aggregator_auto_register):
        """Checking pubsub.psubcribe is called with correct arguments."""
        event_channel = f"external-aggregator/*/{redis_aggregator_auto_register.aggregator_uuid}/events/all"
        channel_dict = {event_channel: redis_aggregator_auto_register._events_callback_dict,
                        "aggregator_response":
                            redis_aggregator_auto_register._aggregator_response_callback,
                        f"external-aggregator/*/"
                        f"{redis_aggregator_auto_register.aggregator_uuid}"
                        f"/response/batch_commands":
                            redis_aggregator_auto_register._batch_response,
                        }
        redis_aggregator_auto_register._subscribe_to_response_channels()
        redis_aggregator_auto_register.pubsub.psubscribe.assert_called_with(**channel_dict)

    def test_batch_response_returns_none(self, redis_aggregator_auto_register):
        """Check if aggregator_uuid does not match as same,then batch_response returns None."""
        data = {"aggregator_uuid": str(uuid.uuid4()), "transaction_id": TRANSACTION_ID}
        message = {"data": json.dumps(data)}
        assert redis_aggregator_auto_register._batch_response(message) is None

    def test_batch_response(self, redis_aggregator_auto_register):
        """Check if batch_response is passed with correct message."""
        transaction_id = redis_aggregator_auto_register._transaction_id_buffer[0]
        data = {"aggregator_uuid": transaction_id, "transaction_id": transaction_id,
                "responses": {transaction_id: ["success"]}}
        message = {"data": json.dumps(data)}
        redis_aggregator_auto_register._batch_response(message)
        assert redis_aggregator_auto_register._transaction_id_buffer == []

    def test_aggregator_response_callback_status_selected(self, redis_aggregator_auto_register):
        """Check if status is selected that adds device_uuid to device_uuid list."""
        transaction_id = redis_aggregator_auto_register._transaction_id_buffer[0]
        device_uuid = str(uuid.uuid4())
        data = {"device_uuid": device_uuid, "transaction_id": transaction_id, "status": "SELECTED"}
        message = {"data": json.dumps(data)}
        redis_aggregator_auto_register._aggregator_response_callback(message)
        assert redis_aggregator_auto_register._transaction_id_buffer == []
        assert redis_aggregator_auto_register.device_uuid_list == [device_uuid]

    def test_aggregator_response_callback_status_unselected(self, redis_aggregator_auto_register):
        """Check if status is unselected that removes device_uuid to device_uuid list."""
        transaction_id = redis_aggregator_auto_register._transaction_id_buffer[0]
        device_uuid = str(uuid.uuid4())
        data = {"device_uuid": device_uuid, "transaction_id": transaction_id,
                "status": "UNSELECTED"}
        message = {"data": json.dumps(data)}
        redis_aggregator_auto_register._aggregator_response_callback(message)
        assert redis_aggregator_auto_register._transaction_id_buffer == []
        assert redis_aggregator_auto_register.device_uuid_list == []

    def test_events_callback_dict_if_event_market(self, redis_aggregator_auto_register):
        """Check if event is market then calls on_market_cycle with correct message."""
        transaction_id = redis_aggregator_auto_register._transaction_id_buffer[0]
        area_id = str(uuid.uuid4())
        device_uuid = str(uuid.uuid4())
        data = {"device_uuid": device_uuid, "transaction_id": transaction_id,
                "event": "market", "grid_tree": {area_id: {"area_name": "market"}}}
        message = {"data": json.dumps(data)}
        redis_aggregator_auto_register._events_callback_dict(message)
        assert redis_aggregator_auto_register.area_name_uuid_mapping == {"market": [area_id]}

    def test_events_callback_dict_if_event_tick(self, redis_aggregator_auto_register):
        """Check if event is tick then calls _on_tick with correct message."""
        redis_aggregator_auto_register._on_tick = MagicMock()
        transaction_id = redis_aggregator_auto_register._transaction_id_buffer[0]
        area_id = str(uuid.uuid4())
        device_uuid = str(uuid.uuid4())
        data = {"device_uuid": device_uuid, "transaction_id": transaction_id,
                "event": "tick", "slot_completion": "50%",
                "grid_tree": {area_id: {"area_name": "market"}}}
        message = {"data": json.dumps(data)}
        payload = json.loads(message["data"])
        redis_aggregator_auto_register._events_callback_dict(message)
        redis_aggregator_auto_register._on_tick.assert_called_with(payload)

    def test_events_callback_dict_if_event_trade(self, redis_aggregator_auto_register):
        """Check if event is trade then calls _on_trade with correct message."""
        redis_aggregator_auto_register._on_trade = MagicMock()
        transaction_id = redis_aggregator_auto_register._transaction_id_buffer[0]
        area_id = str(uuid.uuid4())
        device_uuid = str(uuid.uuid4())
        data = {"device_uuid": device_uuid, "transaction_id": transaction_id,
                "event": "trade", "trade_list": [{"trade_price": 2,
                                                  "traded_energy": 23,
                                                  "buyer": "anonymous"}],
                "grid_tree": {area_id: {"area_name": "market"}}}
        message = {"data": json.dumps(data)}
        payload = json.loads(message["data"])
        redis_aggregator_auto_register._events_callback_dict(message)
        redis_aggregator_auto_register._on_trade.assert_called_with(payload)

    def test_events_callback_dict_if_event_finish(self, redis_aggregator_auto_register):
        """Check if event is finish then calls _on_finish with correct message."""
        redis_aggregator_auto_register._on_finish = MagicMock()
        transaction_id = redis_aggregator_auto_register._transaction_id_buffer[0]
        area_id = str(uuid.uuid4())
        device_uuid = str(uuid.uuid4())
        data = {"device_uuid": device_uuid, "transaction_id": transaction_id,
                "event": "finish",
                "grid_tree": {area_id: {"area_name": "market"}}}
        message = {"data": json.dumps(data)}
        payload = json.loads(message["data"])
        redis_aggregator_auto_register._events_callback_dict(message)
        redis_aggregator_auto_register._on_finish.assert_called_with(payload)

    def test_create_aggregator(self, redis_aggregator_auto_register):
        """Check if redis_db.publish is called with correct arguments to create aggregator."""
        redis_aggregator_auto_register.redis_db.publish = MagicMock()
        redis_aggregator_auto_register._create_aggregator(is_blocking=False)
        transaction_id = redis_aggregator_auto_register._transaction_id_buffer[1]
        data = {"name": AGGREGATOR_NAME, "type": "CREATE",
                "transaction_id": transaction_id}
        redis_aggregator_auto_register.redis_db.publish.assert_called_with(
            "aggregator", json.dumps(data))

    @patch("d3a_api_client.redis_aggregator.wait_until_timeout_blocking",
           side_effect=AssertionError)
    def test_create_aggregator_throws_exception(self,
                                                mock_wait_until_timeout_blocking,
                                                redis_aggregator_auto_register):
        """Check if aggregator is already created then throws exception."""
        with pytest.raises(RedisAPIException,
                           match="API registration process timed out."):
            redis_aggregator_auto_register._create_aggregator(is_blocking=True)

    def test_delete_aggregator(self, redis_aggregator_auto_register):
        """Check if redis_db.publish is called with correct arguments to delete aggregator."""
        redis_aggregator_auto_register.redis_db.publish = MagicMock()
        redis_aggregator_auto_register.delete_aggregator(is_blocking=False)
        transaction_id = redis_aggregator_auto_register._transaction_id_buffer[1]
        aggregator_id = redis_aggregator_auto_register._transaction_id_buffer[0]
        data = {"name": AGGREGATOR_NAME,
                "aggregator_uuid": aggregator_id,
                "type": "DELETE",
                "transaction_id": transaction_id}
        redis_aggregator_auto_register.redis_db.publish.assert_called_with(
            "aggregator", json.dumps(data))

    @patch("d3a_api_client.redis_aggregator.wait_until_timeout_blocking",
           side_effect=AssertionError)
    def test_delete_aggregator_throws_exception(self, mock_wait_until_timeout_blocking,
                                                redis_aggregator_auto_register):
        """Check if aggregator is already deleted then throws exception."""
        with pytest.raises(RedisAPIException,
                           match="API has timed out."):
            redis_aggregator_auto_register.delete_aggregator(is_blocking=True)

    def test_all_uuids_in_selected_device_uuid_list_throw_exception(
            self,
            redis_aggregator_auto_register):
        """Check if device_uuid not present is device_uuid list throws exception."""
        device_uuid = str(uuid.uuid4())
        uuid_list = [device_uuid]
        with pytest.raises(Exception,
                           match=f"{device_uuid} not in list of selected device uuids"):
            redis_aggregator_auto_register._all_uuids_in_selected_device_uuid_list(uuid_list)

    @patch("d3a_api_client.redis_aggregator.RedisAggregator.commands_buffer_length")
    def test_execute_batch_commands(self, moct_commands_buffer_length
                                    , redis_aggregator_auto_register):
        """Check if redis_db.publish is called with correct arguments. """
        redis_aggregator_auto_register.redis_db.publish = MagicMock()
        redis_aggregator_auto_register.execute_batch_commands(is_blocking=False)
        transaction_id = redis_aggregator_auto_register._transaction_id_buffer[1]
        aggregator_id = redis_aggregator_auto_register._transaction_id_buffer[0]
        batch_command_dict = redis_aggregator_auto_register._client_command_buffer.execute_batch()
        batched_command = {"type": "BATCHED", "transaction_id": transaction_id,
                           "aggregator_uuid": aggregator_id,
                           "batch_commands": batch_command_dict}
        batch_channel = f"external//aggregator/{aggregator_id}/batch_commands"
        redis_aggregator_auto_register.redis_db.publish.assert_called_with(
            batch_channel, json.dumps(batched_command))

    @patch("d3a_api_client.redis_aggregator.RedisAggregator.commands_buffer_length")
    @patch("d3a_api_client.redis_aggregator.wait_until_timeout_blocking",
           side_effect=AssertionError)
    def test_execute_batch_commands_throws_exception(self,
                                                     mock_commands_buffer_length,
                                                     mock_wait_until_timeout_blocking,
                                                     redis_aggregator_auto_register):
        """Check if redis_db.publish is not called with correct arguments throws exception."""
        with pytest.raises(RedisAPIException, match="API registration process timed out."):
            redis_aggregator_auto_register.execute_batch_commands(is_blocking=True)

    def test_calculate_grid_fee(self, redis_aggregator_auto_register):
        """Check if calculate grid fee is called with correct arguments."""
        redis_aggregator_auto_register.grid_fee_calculation.calculate_grid_fee = MagicMock()
        start_market_or_device_name: str = "PV"
        target_market_or_device_name: str = None
        fee_type: str = "current_market_fee"
        redis_aggregator_auto_register.calculate_grid_fee(start_market_or_device_name,
                                                          target_market_or_device_name,
                                                          fee_type)
        redis_aggregator_auto_register.grid_fee_calculation.calculate_grid_fee.assert_called_with(
            start_market_or_device_name,
            target_market_or_device_name,
            fee_type
        )
