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

import uuid
import json
from unittest.mock import MagicMock, patch
import pytest
from redis import StrictRedis
import d3a_api_client
from d3a_api_client.redis_client_base import RedisClientBase, RedisAPIException

d3a_api_client.redis_client_base.StrictRedis = MagicMock(spec=StrictRedis)
AREA_ID = str(uuid.uuid4())
TRANSACTION_ID = str(uuid.uuid4())
DEVICE_ID = str(uuid.uuid4())


# pylint: disable=protected-access
class TestRedisClientBase:
    """Tests for the redis client base behaviour without connecting to d3a."""

    @staticmethod
    @pytest.fixture()
    def redis_client_auto_register():
        return RedisClientBase(area_id=AREA_ID, autoregister=False)

    @staticmethod
    @patch("d3a_api_client.redis_client_base.StrictRedis.pubsub.psubscribe")
    def test_subscribe_to_response_channels(pubsub_mock, redis_client_auto_register):
        """Check whether the pubsub.psubcribe is called with correct arguments."""
        redis_client_auto_register._subscribe_to_response_channels()
        redis_client_auto_register.pubsub.psubscribe.assert_called_with(
            **{f"{AREA_ID}/response/register_participant": redis_client_auto_register._on_register,
               f"{AREA_ID}/response/unregister_participant":
                   redis_client_auto_register._on_unregister,
               f"{AREA_ID}/*": redis_client_auto_register._on_event_or_response,
               "aggregator_response": redis_client_auto_register._aggregator_response_callback})

    @staticmethod
    def test_aggregator_response_callback(redis_client_auto_register):
        """Check whether the transaction id buffer is popping out and returning the empty list."""
        data = {"transaction_id": TRANSACTION_ID}
        message = {"data": json.dumps(data)}
        redis_client_auto_register._transaction_id_buffer.append(TRANSACTION_ID)
        redis_client_auto_register._aggregator_response_callback(message)
        assert redis_client_auto_register._transaction_id_buffer == []

    @staticmethod
    def test_check_buffer_message_matching_command_and_id(redis_client_auto_register):
        """Check the buffer message matching the command and id and returns as None."""
        data = {"transaction_id": TRANSACTION_ID}
        redis_client_auto_register._blocking_command_responses = (
            {"register": {"transaction_id": TRANSACTION_ID}})
        assert redis_client_auto_register._check_buffer_message_matching_command_and_id(
            data) is None

    @staticmethod
    def test_check_buffer_message_matching_command_and_id_throws_exception(
            redis_client_auto_register):
        """Checks if _check_buffer_message_matching_command_and_id throws
           exception if the message does not contain transaction_id."""
        message = {}
        with pytest.raises(RedisAPIException,
                           match="The answer message does not contain a valid "
                                 "'transaction_id' member."):
            redis_client_auto_register._check_buffer_message_matching_command_and_id(message)

    @staticmethod
    def test_check_buffer_message_matching_command_and_id_throws_another_exception(
            redis_client_auto_register):
        """Check if the buffer message has no matching command response throws an exception."""
        data = {"transaction_id": TRANSACTION_ID}
        with pytest.raises(RedisAPIException,
                           match="There is no matching command response"
                                 " in _blocking_command_responses."):
            redis_client_auto_register._check_buffer_message_matching_command_and_id(data)

    @staticmethod
    def test_register(redis_client_auto_register):
        """Check if the redis.db.publish is called with correct arguments."""
        redis_client_auto_register.redis_db.publish = MagicMock()
        redis_client_auto_register.register(is_blocking=False)
        transaction_id = redis_client_auto_register._blocking_command_responses[
            "register"]["transaction_id"]
        data = {"name": AREA_ID, "transaction_id": transaction_id}
        redis_client_auto_register.redis_db.publish.assert_called_once_with(
            f"{AREA_ID}/register_participant", json.dumps(data))

    @staticmethod
    @patch("d3a_api_client.redis_client_base.wait_until_timeout_blocking",
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

    @staticmethod
    def test_register_self_active_true_throw_exception(redis_client_auto_register):
        """Check whether if is active is true throws exception."""
        with pytest.raises(RedisAPIException,
                           match="API is already registered to the market."):
            redis_client_auto_register.is_active = True
            redis_client_auto_register.register(is_blocking=False)

    @staticmethod
    def test_unregister(redis_client_auto_register):
        """Check whether the redis.db.publish is called with correct arguments."""
        redis_client_auto_register.redis_db.publish = MagicMock()
        redis_client_auto_register.is_active = True
        redis_client_auto_register.unregister(is_blocking=False)
        transaction_id = redis_client_auto_register._blocking_command_responses[
            "unregister"]["transaction_id"]
        data = {"name": AREA_ID, "transaction_id": transaction_id}
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
    @patch("d3a_api_client.redis_client_base.wait_until_timeout_blocking",
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

    @staticmethod
    def test_on_register(redis_client_auto_register):
        """Check the on_register function with correct message that doesn't throw exception."""
        data = {"device_uuid": DEVICE_ID, "transaction_id": TRANSACTION_ID}
        message = {"data": json.dumps(data)}
        redis_client_auto_register._blocking_command_responses = {
            "register": {"transaction_id": TRANSACTION_ID}}
        redis_client_auto_register._on_register(message)
        assert redis_client_auto_register.is_active is True
        assert redis_client_auto_register.area_uuid == DEVICE_ID

    @staticmethod
    def test_on_unregister(redis_client_auto_register):
        """Check the on_unregister function with correct message that doesn't throw exception."""
        data = {"device_uuid": DEVICE_ID, "transaction_id": TRANSACTION_ID,
                "response": "success"}
        message = {"data": json.dumps(data)}
        redis_client_auto_register._blocking_command_responses = {
            "unregister": {"transaction_id": TRANSACTION_ID}}
        redis_client_auto_register._on_unregister(message)
        assert redis_client_auto_register.is_active is False

    @staticmethod
    def test_on_unregister_throws_exception(redis_client_auto_register):
        """Check if exception is raised when response of on_unregister is not
           successful."""
        with pytest.raises(RedisAPIException,
                           match=f"Failed to unregister from market {AREA_ID}."
                                 "Deactivating connection."):
            data = {"device_uuid": DEVICE_ID, "transaction_id": TRANSACTION_ID,
                    "response": "unsuccessful"}
            message = {"data": json.dumps(data)}
            redis_client_auto_register._blocking_command_responses = {
                "unregister": {"transaction_id": TRANSACTION_ID}}
            redis_client_auto_register._on_unregister(message)
            assert redis_client_auto_register.is_active is True

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
            "aggregator", json.dumps(data))

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
    @patch("d3a_api_client.redis_client_base.wait_until_timeout_blocking",
           side_effect=AssertionError)
    def test_select_aggregator_throws_exception_if_no_d3a_is_running(
            mock_wait_until_timeout_blocking,
            redis_client_auto_register):
        """Check to select aggregator throws an exception when no d3a is running."""
        aggregator_uuid = str(uuid.uuid4())
        with pytest.raises(RedisAPIException,
                           match="API has timed out."):
            redis_client_auto_register.area_uuid = str(uuid.uuid4())
            redis_client_auto_register.select_aggregator(
                aggregator_uuid=aggregator_uuid)
