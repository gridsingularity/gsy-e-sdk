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


# pylint: disable=protected-access
class TestRedisClientBase:
    """Tests for the redis client base behaviour without connecting to d3a."""

    @pytest.fixture()
    def redis_client_auto_register(self):
        return RedisClientBase(area_id=AREA_ID, autoregister=False)

    @patch("d3a_api_client.redis_client_base.StrictRedis.pubsub.psubscribe")
    def test_subscribe_to_response_channels(self, pubsub_mock, redis_client_auto_register):
        """Checks whether the pubsub.psubcribe is called with correct arguments."""
        RedisClientBase._subscribe_to_response_channels = MagicMock()
        redis_client_auto_register._subscribe_to_response_channels()
        redis_client_auto_register.pubsub.psubscribe.assert_called_with(
            **{f"{AREA_ID}/response/register_participant": redis_client_auto_register._on_register,
               f"{AREA_ID}/response/unregister_participant":
                   redis_client_auto_register._on_unregister,
               f"{AREA_ID}/*": redis_client_auto_register._on_event_or_response,
               "aggregator_response": redis_client_auto_register._aggregator_response_callback})

    def test_aggregator_response_callback(self, redis_client_auto_register):
        """Checks whether the transaction id buffer is popping out and returning the empty list."""
        message = {"data": {"transaction_id": f"{TRANSACTION_ID}"}}
        message["data"] = json.dumps(message["data"])
        redis_client_auto_register._transaction_id_buffer.append(TRANSACTION_ID)
        redis_client_auto_register._aggregator_response_callback(message)
        assert redis_client_auto_register._transaction_id_buffer == []

    def test_check_buffer_message_matching_command_and_id(self, redis_client_auto_register):
        """Checks the buffer message matching the command and id and returns as None."""
        message = {"data": {"transaction_id": TRANSACTION_ID}}
        redis_client_auto_register._blocking_command_responses = (
            {"register": {"transaction_id": f"{TRANSACTION_ID}"}})
        assert redis_client_auto_register._check_buffer_message_matching_command_and_id(
            message["data"]) is None

    def test_check_buffer_message_matching_command_and_id_throws_exception(self,redis_client_auto_register):
        """Checks buffer message matching command and id does
           contain transaction id throws exception."""
        message = {}
        with pytest.raises(RedisAPIException,
                           match="The answer message does not contain a valid "
                                 "'transaction_id' member."):
            redis_client_auto_register._check_buffer_message_matching_command_and_id(message)

    def test_check_buffer_message_matching_command_and_id_throws_another_exception(self, redis_client_auto_register):
        """Checks buffer message matching command and id throws exception."""
        message = {"data": {"transaction_id": TRANSACTION_ID}}
        with pytest.raises(RedisAPIException,
                           match="There is no matching command response"
                                 " in _blocking_command_responses."):
            redis_client_auto_register._check_buffer_message_matching_command_and_id(message["data"])

    def test_register(self, redis_client_auto_register):
        """Checks whether the redis.db.publish is called with correct arguments
           and is connected via redis and no d3a is running."""
        data = {"name": AREA_ID, "transaction_id": AREA_ID}
        redis_client_auto_register.redis_db.publish = MagicMock()
        redis_client_auto_register.register(is_blocking=False)
        redis_client_auto_register.redis_db.publish.assert_called_once_with(
            f"{AREA_ID}/register_participant", json.dumps(data))

    @patch("d3a_api_client.redis_client_base.wait_until_timeout_blocking", side_effect=AssertionError)
    def test_register_blocking_true_throws_exception(self, mock_wait_until_timeout_blocking,
                                                     redis_client_auto_register):
        """Checks whether the if is blocking is set to true throws exception
           with side effect of assertion error,
           when the user tries to register."""
        with pytest.raises(RedisAPIException,
                           match="API registration process timed out. "
                                 "Server will continue processing "
                                 "your request on the background "
                                 "and will notify you as soon as the "
                                 "registration has been completed."):
            redis_client_auto_register.register(is_blocking=True)
            mock_wait_until_timeout_blocking.assert_called()

    def test_register_blocking_false_throw_exception(self, redis_client_auto_register):
        """Checks whether the if is blocking is set to false throws exception
           that user is already registered"""
        with pytest.raises(RedisAPIException,
                           match="API is already registered to the market."):
            self.redis_client_base = redis_client_auto_register
            self.redis_client_base.is_active = True
            self.redis_client_base.register(is_blocking=False)

    def test_unregister(self, redis_client_auto_register):
        """Checks whether the redis.db.publish is called with correct arguments
           and is connected via redis and no d3a is running."""
        data = {"name": AREA_ID, "transaction_id": AREA_ID}
        self.redis_client = redis_client_auto_register
        self.redis_client.redis_db.publish = MagicMock()
        self.redis_client.is_active = True
        self.redis_client.unregister(is_blocking=False)
        self.redis_client.redis_db.publish.assert_called_once_with(
            f"{AREA_ID}/unregister_participant",
            json.dumps(data))

    def test_unregister_blocking_is_active_false_throws_exception(self, redis_client_auto_register):
        """Checks if is active set to false throws exception when user is already unregistered."""
        with pytest.raises(RedisAPIException,
                           match="API is already unregistered from the market."):
            self.redis_client = redis_client_auto_register
            self.redis_client.is_active = False
            self.redis_client.unregister(is_blocking=True)

    @patch("d3a_api_client.redis_client_base.wait_until_timeout_blocking", side_effect=AssertionError)
    def test_unregister_blocking_is_active_true_throws_exception(self,
                                                                 mock_wait_until_timeout_blocking,
                                                                 redis_client_auto_register):
        """Checks if is active set to true throws exception with side
          effect of assertion error when user tries to unregister"""
        with pytest.raises(RedisAPIException,
                           match="API unregister process timed out. "
                                 "Server will continue processing "
                                 "your request on the background and "
                                 "will notify you as soon as "
                                 "the unregistration has been completed."):
            self.redis_client = redis_client_auto_register
            self.redis_client.is_active = True
            self.redis_client.unregister(is_blocking=True)
            mock_wait_until_timeout_blocking.assert_called()

    def test_on_register(self, redis_client_auto_register):
        """Assigning the correct arguments to blocking command response and
           checking by calling on_register function with correct message
           and should not throw an exception."""
        data = {"device_uuid": TRANSACTION_ID, "transaction_id": TRANSACTION_ID}
        message = {"data": json.dumps(data)}
        redis_client_auto_register._blocking_command_responses = {
            "register": {"transaction_id": f"{TRANSACTION_ID}"}}
        redis_client_auto_register._on_register(message)

    def test_on_unregister(self, redis_client_auto_register):
        """Assigning the correct arguments to blocking command response and
           checking by calling on_unregister function with correct message
           and should not throw an exception."""
        data = {"device_uuid": TRANSACTION_ID, "transaction_id": TRANSACTION_ID,
                            "response": "success"}
        message = {"data": json.dumps(data)}
        redis_client_auto_register._blocking_command_responses = {
            "unregister": {"transaction_id": f"{TRANSACTION_ID}"}}
        redis_client_auto_register._on_unregister(message)

    def test_on_unregister_throws_exception(self, redis_client_auto_register):
        """Checks if response is not success should throw exception """
        with pytest.raises(RedisAPIException,
                           match=f"Failed to unregister from market {AREA_ID}."
                                 "Deactivating connection."):
            message = {"data": {"device_uuid": TRANSACTION_ID, "transaction_id": TRANSACTION_ID,
                                "response": "unsuccessful"}}
            message["data"] = json.dumps(message["data"])
            redis_client_auto_register._blocking_command_responses = {
                "unregister": {"transaction_id": f"{TRANSACTION_ID}"}}
            redis_client_auto_register._on_unregister(message)

    def test_select_aggregator(self, redis_client_auto_register):
        """Checks if redis_db.publish is called with correct arguments
           when user tries to select aggregator."""
        aggregator_uuid = str(uuid.uuid4())
        self.redis_client = redis_client_auto_register
        self.redis_client.redis_db.publish = MagicMock()
        self.redis_client.area_uuid = AREA_ID
        self.redis_client.select_aggregator(aggregator_uuid, is_blocking=False)
        transaction_id = self.redis_client._transaction_id_buffer[0]
        data = {"aggregator_uuid": aggregator_uuid,
                "device_uuid": AREA_ID,
                "type": "SELECT",
                "transaction_id": transaction_id}
        self.redis_client.redis_db.publish.assert_called_once_with("aggregator", json.dumps(data))

    def test_select_aggregator_self_area_uuid_none_throws_exception(self,
                                                                    redis_client_auto_register):
        """Checks when the device is not registered to the aggregator throws exception."""
        aggregator_uuid = str(uuid.uuid4())
        with pytest.raises(RedisAPIException,
                           match="The device/market has not ben registered yet,"
                                 " can not select an aggregator"):
            redis_client_auto_register.select_aggregator(
                aggregator_uuid=aggregator_uuid, is_blocking=True)

    @patch("d3a_api_client.redis_client_base.wait_until_timeout_blocking", side_effect=AssertionError)
    def test_select_aggregator_self_area_uuid_not_none_throws_exception(self,
                                                                        mock_wait_until_timeout_blocking,
                                                                        redis_client_auto_register):
        """Checks when the device is registered aggregator with area_uuid
           is not none throws exception."""
        aggregator_uuid = str(uuid.uuid4())
        with pytest.raises(RedisAPIException,
                           match="API has timed out."):
            self.redis_client = redis_client_auto_register
            self.redis_client.area_uuid = str(uuid.uuid4())
            redis_client_auto_register.select_aggregator(
                aggregator_uuid=aggregator_uuid, is_blocking=True)
            mock_wait_until_timeout_blocking.assert_called()
