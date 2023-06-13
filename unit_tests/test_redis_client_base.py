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
import uuid
from unittest.mock import MagicMock, patch

import pytest
from gsy_framework.redis_channels import ExternalStrategyChannels, AggregatorChannels
from redis import Redis

from gsy_e_sdk.redis_client_base import RedisClientBase, RedisAPIException

AREA_ID = str(uuid.uuid4())
TRANSACTION_ID = str(uuid.uuid4())
DEVICE_ID = str(uuid.uuid4())


# pylint: disable=protected-access
class TestRedisClientBase:
    """Tests for the redis client base behaviour without connecting to d3a."""

    @staticmethod
    @pytest.fixture()
    @patch("gsy_e_sdk.redis_client_base.Redis")
    def redis_client_auto_register(strict_redis_mock):
        """Create the fixture for redis client base."""
        strict_redis_mock.return_value = MagicMock(spec=Redis)
        return RedisClientBase(area_id=AREA_ID, autoregister=False)

    @staticmethod
    def test_subscribe_to_response_channels(redis_client_auto_register):
        """Check whether the pubsub.psubcribe is called with correct arguments."""
        channel_names = ExternalStrategyChannels(False, "", asset_uuid=AREA_ID, asset_name=AREA_ID)
        redis_client_auto_register._subscribe_to_response_channels()
        redis_client_auto_register.pubsub.psubscribe.assert_called_with(
            **{channel_names.register_response: redis_client_auto_register._on_register,
               channel_names.unregister_response: redis_client_auto_register._on_unregister,
               f"{AREA_ID}/*": redis_client_auto_register._on_event_or_response,
               AggregatorChannels("", "").response:
                   redis_client_auto_register._aggregator_response_callback})

    @staticmethod
    def test_aggregator_response_callback(redis_client_auto_register):
        """Check whether the transaction id buffer is popping out and returning the empty list."""
        data = {"transaction_id": TRANSACTION_ID}
        message = {"data": json.dumps(data)}
        redis_client_auto_register._transaction_id_buffer.append(TRANSACTION_ID)
        redis_client_auto_register._aggregator_response_callback(message)
        assert redis_client_auto_register._transaction_id_buffer == []

    @staticmethod
    @patch("uuid.uuid4", return_value="some-transaction-uuid")
    def test_check_buffer_message_matching_command_and_id(uuid_mock, redis_client_auto_register):
        """Check the buffer message matching the command and id and returns as None."""
        redis_client_auto_register.register(is_blocking=False)
        data = {"name": AREA_ID, "transaction_id": uuid_mock()}
        assert redis_client_auto_register._check_buffer_message_matching_command_and_id(
            data) is None

    @staticmethod
    def test_check_buffer_message_matching_command_and_id_throws_exception(
            redis_client_auto_register):
        """Check exception is raised if the message does not contain transaction_id."""
        message = {}
        with pytest.raises(RedisAPIException,
                           match="The answer message does not contain a valid "
                                 "'transaction_id' member."):
            redis_client_auto_register._check_buffer_message_matching_command_and_id(message)

    @staticmethod
    def test_check_buffer_message_matching_command_and_id_without_command_throws_exception(
            redis_client_auto_register):
        """Check if the buffer message has no matching command response throws an exception."""
        data = {"transaction_id": TRANSACTION_ID}
        with pytest.raises(RedisAPIException,
                           match="There is no matching command response"
                                 " in _blocking_command_responses."):
            redis_client_auto_register._check_buffer_message_matching_command_and_id(data)

    @staticmethod
    @patch("uuid.uuid4", return_value="some-transaction-uuid")
    def test_register(uuid_mock, redis_client_auto_register):
        """Check if the redis.db.publish is called with correct arguments."""
        redis_client_auto_register.register(is_blocking=False)
        data = {"name": AREA_ID, "transaction_id": uuid_mock()}
        redis_client_auto_register.redis_db.publish.assert_called_once_with(
            f"{AREA_ID}/register_participant", json.dumps(data))

    @staticmethod
    @patch("gsy_e_sdk.redis_client_base.wait_until_timeout_blocking",
           side_effect=AssertionError)
    def test_register_self_active_false_throws_exception(mock_wait_until_timeout_blocking,
                                                         redis_client_auto_register):
        """Check whether if is active is false throws exception."""
        with pytest.raises(RedisAPIException,
                           match="API registration process timed out. "
                                 "Server will continue processing "
                                 "your request on the background "
                                 "and will notify you as soon as the "
                                 "registration has been completed."):
            redis_client_auto_register.register(is_blocking=True)
        mock_wait_until_timeout_blocking.assert_called()

    @staticmethod
    def test_register_self_active_true_throws_exception(redis_client_auto_register):
        """Check whether if is active is true throws exception."""
        with pytest.raises(RedisAPIException,
                           match="API is already registered to the market."):
            redis_client_auto_register.is_active = True
            redis_client_auto_register.register(is_blocking=False)

    @staticmethod
    @patch("uuid.uuid4", return_value="some-transaction-uuid")
    def test_unregister(uuid_mock, redis_client_auto_register):
        """Check whether the redis.db.publish is called with correct arguments."""
        redis_client_auto_register.is_active = True
        redis_client_auto_register.unregister(is_blocking=False)
        data = {"name": AREA_ID, "transaction_id": uuid_mock()}
        redis_client_auto_register.redis_db.publish.assert_called_once_with(
            f"{AREA_ID}/unregister_participant",
            json.dumps(data))

    @staticmethod
    def test_unregister_is_active_false_throws_exception(redis_client_auto_register):
        """Check if is active set to false throws exception when user is already unregistered."""
        with pytest.raises(RedisAPIException,
                           match="API is already unregistered from the market."):
            redis_client_auto_register.is_active = False
            redis_client_auto_register.unregister(is_blocking=True)

    @staticmethod
    @patch("gsy_e_sdk.redis_client_base.wait_until_timeout_blocking",
           side_effect=AssertionError)
    def test_unregister_is_active_true_throws_exception(
            mock_wait_until_timeout_blocking,
            redis_client_auto_register):
        """Check if is active set to true throws exception."""
        with pytest.raises(RedisAPIException,
                           match="API unregister process timed out. "
                                 "Server will continue processing "
                                 "your request on the background and "
                                 "will notify you as soon as "
                                 "the unregistration has been completed."):
            redis_client_auto_register.is_active = True
            redis_client_auto_register.unregister(is_blocking=True)
        mock_wait_until_timeout_blocking.assert_called()

    @staticmethod
    @patch("uuid.uuid4", return_value="some-transaction-uuid")
    @patch("gsy_e_sdk.redis_client_base.logging")
    def test_on_register(logging_mock, uuid_mock, redis_client_auto_register):
        """Check the on_register function with correct message that doesn't throw exception."""
        redis_client_auto_register.on_register = MagicMock()
        redis_client_auto_register.register(is_blocking=False)
        data = {"name": AREA_ID, "device_uuid": DEVICE_ID, "transaction_id": uuid_mock()}
        message = {"data": json.dumps(data)}
        redis_client_auto_register._on_register(message)

        logging_mock.info.assert_called_with("%s was registered", AREA_ID)
        redis_client_auto_register.on_register.assert_called()
        assert redis_client_auto_register.is_active is True
        assert redis_client_auto_register.area_uuid == DEVICE_ID

    @staticmethod
    @patch("uuid.uuid4", return_value="some-transaction-uuid")
    def test_on_unregister(uuid_mock, redis_client_auto_register):
        """Check the on_unregister function with correct message that doesn't throws exception."""
        redis_client_auto_register.is_active = True
        redis_client_auto_register.unregister(is_blocking=False)
        data = {"name": AREA_ID, "device_id": DEVICE_ID,
                "transaction_id": uuid_mock(), "response": "success"}
        message = {"data": json.dumps(data)}
        redis_client_auto_register._on_unregister(message)
        assert redis_client_auto_register.is_active is False

    @staticmethod
    @patch("uuid.uuid4", return_value="some-transaction-uuid")
    def test_on_unregister_throws_exception(uuid_mock, redis_client_auto_register):
        """Check if exception is raised when response of on_unregister is not successful."""
        with pytest.raises(
                RedisAPIException,
                match=f"Failed to unregister from market {AREA_ID}. Deactivating connection."):
            redis_client_auto_register.is_active = True
            redis_client_auto_register.unregister(is_blocking=False)
            data = {"name": AREA_ID, "device_id": DEVICE_ID,
                    "transaction_id": uuid_mock(), "response": "unsuccessful"}
            message = {"data": json.dumps(data)}
            redis_client_auto_register._on_unregister(message)

        assert redis_client_auto_register.is_active

    @staticmethod
    def test_select_aggregator(redis_client_auto_register):
        """Check if redis_db.publish is called with correct arguments to select aggregator."""
        aggregator_uuid = str(uuid.uuid4())
        redis_client_auto_register.redis_db.publish = MagicMock()
        redis_client_auto_register.area_uuid = AREA_ID
        redis_client_auto_register.select_aggregator(aggregator_uuid, is_blocking=False)
        transaction_id = redis_client_auto_register._transaction_id_buffer[0]
        data = {"aggregator_uuid": aggregator_uuid,
                "device_uuid": AREA_ID,
                "type": "SELECT",
                "transaction_id": transaction_id}
        redis_client_auto_register.redis_db.publish.assert_called_once_with(
            AggregatorChannels.commands, json.dumps(data))

    @staticmethod
    def test_select_aggregator_area_uuid_none_throws_exception(
            redis_client_auto_register):
        """Check if the device/market is not registered to the aggregator throws an exception."""
        aggregator_uuid = str(uuid.uuid4())
        with pytest.raises(RedisAPIException,
                           match="The device/market has not ben registered yet,"
                                 " can not select an aggregator"):
            redis_client_auto_register.select_aggregator(
                aggregator_uuid=aggregator_uuid)

    @staticmethod
    @patch("gsy_e_sdk.redis_client_base.wait_until_timeout_blocking",
           side_effect=AssertionError)
    def test_select_aggregator_throws_exception_if_no_d3a_is_running(
            mock_wait_until_timeout_blocking,
            redis_client_auto_register):
        """Check to select aggregator throws an exception when no d3a is running."""
        aggregator_uuid = str(uuid.uuid4())
        with pytest.raises(RedisAPIException,
                           match="API has timed out."):
            redis_client_auto_register.area_uuid = str(uuid.uuid4())
            redis_client_auto_register.select_aggregator(aggregator_uuid=aggregator_uuid)
        mock_wait_until_timeout_blocking.assert_called()

    @staticmethod
    def test_is_transaction_response_received(redis_client_auto_register):
        """Check the return value of is_transaction_response_received."""
        transaction_id = str(uuid.uuid4())
        redis_client_auto_register._transaction_id_buffer = [transaction_id]
        assert redis_client_auto_register._is_transaction_response_received(
            transaction_id) is False

        redis_client_auto_register._transaction_id_buffer.pop(0)
        assert redis_client_auto_register._is_transaction_response_received(
            transaction_id) is True
