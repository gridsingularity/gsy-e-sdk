import uuid
from unittest import mock

import pytest
from redis import StrictRedis
import d3a_api_client
from d3a_api_client.redis_client_base import RedisClientBase, RedisAPIException
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

area_id = str(uuid.uuid4())
area_uid = None
d3a_api_client.redis_client_base.StrictRedis = MagicMock(spec=StrictRedis)
d3a_api_client.redis_client_base.StrictRedis.publish = MagicMock(spec=StrictRedis)
d3a_api_client.redis_client_base.StrictRedis.pubsub.psubscribe = MagicMock()


@pytest.fixture
def redis_client_auto_register():
    return RedisClientBase(area_id=area_id, autoregister=False)


class TestRedisClientBase:
    @classmethod
    def setup_class(cls):
        cls.obj = RedisClientBase(area_id=area_id, autoregister=False)

    def test_constructor_registry_success(self, pubsub_thread=None):
        assert self.obj.area_id == area_id
        assert self.obj.area_uuid == area_uid
        assert self.obj.is_active == False
        assert self.obj._subscribed_aggregator_response_cb is None
        assert self.obj._subscribe_to_response_channels(pubsub_thread) is None
        assert self.obj._blocking_command_responses == {}
        assert self.obj._transaction_id_buffer == []

    def test_function_subscribe_to_response_channels_success(self):
        RedisClientBase._subscribe_to_response_channels = MagicMock()
        test = RedisClientBase(area_id=area_id, autoregister=False)
        test._subscribe_to_response_channels.assert_called()
        test.pubsub.psubscribe.assert_called()

    def test_function_aggregator_response_callback_success(self):
        RedisClientBase._aggregator_response_callback = MagicMock()
        test = RedisClientBase(area_id=area_id, autoregister=False)
        test._aggregator_response_callback()
        test._aggregator_response_callback.assert_called_once()

    @pytest.fixture()
    def register_fixture(self):
        initial = RedisClientBase.register
        RedisClientBase.register = MagicMock()
        test = RedisClientBase(area_id=area_id, autoregister=False)
        yield test
        RedisClientBase.register = initial

    @pytest.fixture()
    def unregister_fixture(self):
        initial = RedisClientBase.unregister
        RedisClientBase.unregister = MagicMock()
        test = RedisClientBase(area_id=area_id, autoregister=False)
        yield test
        RedisClientBase.unregister = initial

    @pytest.fixture()
    def on_register_fixture(self):
        initial = RedisClientBase._on_register
        RedisClientBase._on_register = MagicMock()
        test = RedisClientBase(area_id=area_id, autoregister=False)
        yield test
        RedisClientBase._on_register = initial

    @pytest.fixture()
    def check_buffer_message_fixture(self):
        initial = RedisClientBase._check_buffer_message_matching_command_and_id
        RedisClientBase._check_buffer_message_matching_command_and_id = MagicMock()
        test = RedisClientBase(area_id=area_id, autoregister=False)
        yield test
        RedisClientBase._check_buffer_message_matching_command_and_id = initial

    @pytest.fixture()
    def check_transaction_id_cached_out_call_success_fixture(self):
        initial = RedisClientBase._check_transaction_id_cached_out
        RedisClientBase._check_transaction_id_cached_out = MagicMock()
        test = RedisClientBase(area_id=area_id, autoregister=False)
        yield test
        RedisClientBase._check_transaction_id_cached_out = initial

    def test_function_check_buffer_message_matching_command_and_id_fixture_call_success(self,
                                                                                        check_buffer_message_fixture):
        check_buffer_message_fixture._check_buffer_message_matching_command_and_id()
        check_buffer_message_fixture._check_buffer_message_matching_command_and_id.assert_called_once()

    def test_function_register_call_success(self, register_fixture):
        register_fixture.register()
        register_fixture.register.assert_called_once()

    def test_function_unregister_call_success(self, unregister_fixture):
        unregister_fixture.unregister()
        unregister_fixture.unregister.assert_called_once()

    def test_function_on_register_call_success(self, on_register_fixture):
        on_register_fixture._on_register()
        on_register_fixture._on_register.assert_called_once()

    def test_function__check_transaction_id_cached_out_call_success(self,
                                                                    check_transaction_id_cached_out_call_success_fixture):
        check_transaction_id_cached_out_call_success_fixture._check_transaction_id_cached_out()
        check_transaction_id_cached_out_call_success_fixture._check_transaction_id_cached_out.assert_called_once()

    def test_check_buffer_message_matching_command_and_id_throws_exception(self):
        message = {}
        with pytest.raises(RedisAPIException,
                           match='The answer message does not contain a valid '
                                 '\'transaction_id\' member.') as ex:
            redis_client_base = RedisClientBase(area_id=area_id, autoregister=False)
            redis_client_base._check_buffer_message_matching_command_and_id(message)

    def test_register_blocking_true_throws_exception(self):
        with pytest.raises(RedisAPIException,
                           match='API registration process timed out. Server will continue processing '
                                 'your request on the background and will notify you as soon as the '
                                 'registration has been completed.') as ex:
            redis_client_base = RedisClientBase(area_id=area_id, autoregister=True)
            redis_client_base.register(is_blocking=True)

    def test_register_blocking_false_published_success(self):
        # arrange
        redis_client_base = RedisClientBase(area_id=area_id, autoregister=False)
        redis_client_base.register(is_blocking=False)
        assert redis_client_base._blocking_command_responses["register"]["name"] == area_id

    def test_register_blocking_false_throw_exception(self, redis_client_auto_register):
        with pytest.raises(RedisAPIException,
                           match='API is already registered to the market.') as ex:
            self.redis_client_base = redis_client_auto_register
            self.redis_client_base.is_active = True
            self.redis_client_base.register(is_blocking=False)

    def test_unregister_blocking_isactive_false_throws_exception(self, redis_client_auto_register):
        with pytest.raises(RedisAPIException,
                           match='API is already unregistered from the market.') as ex:
            self.obj = redis_client_auto_register
            self.obj.is_active = False
            self.obj.unregister(is_blocking=True)

    def test_unregister_blocking_isactive_true_throws_exception(self, redis_client_auto_register):
        with pytest.raises(RedisAPIException,
                           match='API unregister process timed out. Server will continue processing '
                                 'your request on the background and will notify you as soon as '
                                 'the unregistration has been completed.') as ex:
            self.obj = redis_client_auto_register
            self.obj.is_active = True
            self.obj.unregister(is_blocking=True)

    def test_select_aggregator_self_area_uuid_none_throws_exception(self, redis_client_auto_register):
        aggregator_uuid = str(uuid.uuid4())
        with pytest.raises(RedisAPIException,
                           match='The device/market has not ben registered yet, can not select an aggregator') as ex:
            redis_client_auto_register.select_aggregator(aggregator_uuid=aggregator_uuid, is_blocking=True)

    def test_select_aggregator_self_area_uuid_not_none_throws_exception(self, redis_client_auto_register):
        aggregator_uuid = str(uuid.uuid4())
        with pytest.raises(RedisAPIException,
                           match='API has timed out.') as ex:
            self.obj = redis_client_auto_register
            self.obj.area_uuid = str(uuid.uuid4())
            redis_client_auto_register.select_aggregator(aggregator_uuid=aggregator_uuid, is_blocking=True)

    def test_unselect_aggregator_throws_exception(self, redis_client_auto_register):
        aggregator_uuid = str(uuid.uuid4())
        with pytest.raises(NotImplementedError,
                           match='unselect_aggregator hasn\'t been implemented yet.') as ex:
            self.obj = redis_client_auto_register
            redis_client_auto_register.unselect_aggregator(aggregator_uuid=aggregator_uuid)
