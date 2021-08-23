import json
import uuid
import pytest
from redis import StrictRedis
import d3a_api_client
from d3a_api_client.redis_client_base import RedisClientBase, RedisAPIException
from unittest.mock import MagicMock

area_id = str(uuid.uuid4())
transactionID = str(uuid.uuid4())
d3a_api_client.redis_client_base.StrictRedis = MagicMock(spec=StrictRedis)
d3a_api_client.redis_client_base.StrictRedis.publish = MagicMock(spec=StrictRedis)
d3a_api_client.redis_client_base.StrictRedis.pubsub.psubscribe = MagicMock()


@pytest.fixture
def redis_client_auto_register():
    return RedisClientBase(area_id=area_id, autoregister=False)


class TestRedisClientBase:

    def test_function__subscribe_to_response_channels(self, redis_client_auto_register):
        RedisClientBase._subscribe_to_response_channels = MagicMock()
        redis_client_auto_register._subscribe_to_response_channels()
        redis_client_auto_register.pubsub.psubscribe.assert_called_with(
            **{f"{area_id}/response/register_participant": redis_client_auto_register._on_register,
               f"{area_id}/response/unregister_participant": redis_client_auto_register._on_unregister,
               f"{area_id}/*": redis_client_auto_register._on_event_or_response,
               "aggregator_response": redis_client_auto_register._aggregator_response_callback})

    def test_function_aggregator_response_callback(self, redis_client_auto_register):
        message = {'data': {"transaction_id": f"{transactionID}"}}
        message["data"] = json.dumps(message['data'])
        redis_client = redis_client_auto_register
        redis_client._transaction_id_buffer.append(transactionID)
        redis_client._aggregator_response_callback(message)
        assert redis_client._transaction_id_buffer == []

    def test_function_check_buffer_message_matching_command(self, redis_client_auto_register):
        message = dict()
        message["data"] = {"transaction_id": f"{transactionID}"}
        redis_client_auto_register._blocking_command_responses = {"register": {"transaction_id": f"{transactionID}"}}
        assert redis_client_auto_register._check_buffer_message_matching_command_and_id(message['data']) is None

    def test_check_buffer_message_matching_command_and_id_throws_exception(self, redis_client_auto_register):
        message = {}
        with pytest.raises(RedisAPIException,
                           match='The answer message does not contain a valid '
                                 '\'transaction_id\' member.') as ex:
            redis_client = redis_client_auto_register
            redis_client._check_buffer_message_matching_command_and_id(message)

    def test_check_buffer_message_matching_command_and_id_throws_another_exception(self, redis_client_auto_register):
        message = dict()
        message["data"] = {"transaction_id": "1234"}
        with pytest.raises(RedisAPIException,
                           match='There is no matching command response in _blocking_command_responses.') as ex:
            redis_client = redis_client_auto_register
            redis_client_auto_register._blocking_command_responses = {
                "register": {"transaction_id": f"{transactionID}"}}
            redis_client._check_buffer_message_matching_command_and_id(message['data'])

    def test_function_register(self, redis_client_auto_register):
        data = {"name": area_id, "transaction_id": area_id}
        redis_client = redis_client_auto_register
        redis_client.redis_db.publish = MagicMock()
        redis_client.register(is_blocking=False)
        redis_client.redis_db.publish.assert_called_once_with(f'{area_id}/register_participant', json.dumps(data))

    def test_register_blocking_true_throws_exception(self, redis_client_auto_register):
        with pytest.raises(RedisAPIException,
                           match='API registration process timed out. Server will continue processing '
                                 'your request on the background and will notify you as soon as the '
                                 'registration has been completed.') as ex:
            redis_client = redis_client_auto_register
            redis_client.register(is_blocking=True)

    def test_register_blocking_false_throw_exception(self, redis_client_auto_register):
        with pytest.raises(RedisAPIException,
                           match='API is already registered to the market.') as ex:
            self.redis_client_base = redis_client_auto_register
            self.redis_client_base.is_active = True
            self.redis_client_base.register(is_blocking=False)

    def test_function_unregister(self, redis_client_auto_register):
        data = {"name": area_id, "transaction_id": area_id}
        self.redis_client = redis_client_auto_register
        self.redis_client.redis_db.publish = MagicMock()
        self.redis_client.is_active = True
        self.redis_client.unregister(is_blocking=False)
        self.redis_client.redis_db.publish.assert_called_once_with(f'{area_id}/unregister_participant',
                                                                   json.dumps(data))

    def test_unregister_blocking_is_active_false_throws_exception(self, redis_client_auto_register):
        with pytest.raises(RedisAPIException,
                           match='API is already unregistered from the market.') as ex:
            self.redis_client = redis_client_auto_register
            self.redis_client.is_active = False
            self.redis_client.unregister(is_blocking=True)

    def test_unregister_blocking_is_active_true_throws_exception(self, redis_client_auto_register):
        with pytest.raises(RedisAPIException,
                           match='API unregister process timed out. Server will continue processing '
                                 'your request on the background and will notify you as soon as '
                                 'the unregistration has been completed.') as ex:
            self.redis_client = redis_client_auto_register
            self.redis_client.is_active = True
            self.redis_client.unregister(is_blocking=True)

    def test_on_register(self, redis_client_auto_register):
        message = dict()
        message['data'] = {"device_uuid": f"{transactionID}", "transaction_id": f"{transactionID}"}
        message["data"] = json.dumps(message['data'])
        redis_client = redis_client_auto_register
        redis_client._blocking_command_responses = {"register": {"transaction_id": f"{transactionID}"}}
        redis_client._on_register(message)

    def test_on_unregister(self, redis_client_auto_register):
        message = dict()
        message['data'] = {"device_uuid": f"{transactionID}", "transaction_id": f"{transactionID}",
                           "response": "success"}
        message["data"] = json.dumps(message['data'])
        redis_client = redis_client_auto_register
        redis_client._blocking_command_responses = {"unregister": {"transaction_id": f"{transactionID}"}}
        redis_client._on_unregister(message)

    def test_on_unregister_throws_exception(self, redis_client_auto_register):
        with pytest.raises(RedisAPIException, match=f'Failed to unregister from market {area_id}.'
                                                    'Deactivating connection.') as ex:
            message = dict()
            message['data'] = {"device_uuid": f"{transactionID}", "transaction_id": f"{transactionID}",
                               "response": "unsuccess"}
            message["data"] = json.dumps(message['data'])
            redis_client = redis_client_auto_register
            redis_client._blocking_command_responses = {"unregister": {"transaction_id": f"{transactionID}"}}
            redis_client._on_unregister(message)

    def test_on_event_or_response(self, redis_client_auto_register):
        message = dict()
        message['data'] = {"device_uuid": f"{transactionID}", "transaction_id": f"{transactionID}"}
        message["data"] = json.dumps(message['data'])
        redis_client = redis_client_auto_register
        redis_client._on_event_or_response(message)

    def test_function_select_aggregator(self, redis_client_auto_register):
        aggregator_uuid = str(uuid.uuid4())
        self.redis_client = redis_client_auto_register
        self.redis_client.redis_db.publish = MagicMock()
        self.redis_client.area_uuid = area_id
        self.redis_client.select_aggregator(aggregator_uuid, is_blocking=False)
        transaction_id = self.redis_client._transaction_id_buffer[0]
        data = {"aggregator_uuid": aggregator_uuid,
                "device_uuid": area_id,
                "type": "SELECT",
                "transaction_id": transaction_id}
        self.redis_client.redis_db.publish.assert_called_once_with("aggregator", json.dumps(data))

    def test_select_aggregator_self_area_uuid_none_throws_exception(self, redis_client_auto_register):
        aggregator_uuid = str(uuid.uuid4())
        with pytest.raises(RedisAPIException,
                           match='The device/market has not ben registered yet, can not select an aggregator') as ex:
            redis_client_auto_register.select_aggregator(aggregator_uuid=aggregator_uuid, is_blocking=True)

    def test_select_aggregator_self_area_uuid_not_none_throws_exception(self, redis_client_auto_register):
        aggregator_uuid = str(uuid.uuid4())
        with pytest.raises(RedisAPIException,
                           match='API has timed out.') as ex:
            self.redis_client = redis_client_auto_register
            self.redis_client.area_uuid = str(uuid.uuid4())
            redis_client_auto_register.select_aggregator(aggregator_uuid=aggregator_uuid, is_blocking=True)

    def test_unselect_aggregator_throws_exception(self, redis_client_auto_register):
        aggregator_uuid = str(uuid.uuid4())
        with pytest.raises(NotImplementedError,
                           match='unselect_aggregator hasn\'t been implemented yet.') as ex:
            redis_client = redis_client_auto_register
            redis_client.unselect_aggregator(aggregator_uuid=aggregator_uuid)

