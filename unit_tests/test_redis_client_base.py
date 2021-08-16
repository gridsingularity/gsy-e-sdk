import uuid
import pytest
from redis import StrictRedis
import d3a_api_client
from d3a_api_client.redis_client_base import RedisClientBase, RedisAPIException
from unittest.mock import MagicMock
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

    def test_constructor_registry_success(self, redis_client_auto_register, pubsub_thread=None):
        assert redis_client_auto_register.area_id == area_id
        assert redis_client_auto_register.area_uuid == area_uid
        assert redis_client_auto_register.is_active == False
        assert redis_client_auto_register._subscribed_aggregator_response_cb is None
        assert redis_client_auto_register._subscribe_to_response_channels(pubsub_thread) is None
        assert redis_client_auto_register._blocking_command_responses == {}
        assert redis_client_auto_register._transaction_id_buffer == []

    @patch('d3a_api_client.redis_client_base.RedisClientBase._subscribe_to_response_channels')
    def test_function_subscribe_to_response_channels_sucess(self, mock_funct):
        f = mock_funct()
        mock_funct.assert_called_once()

    @patch('d3a_api_client.redis_client_base.RedisClientBase._aggregator_response_callback')
    def test_function_aggregator_response_callback_success(self, mock_funct):
        f = mock_funct()
        mock_funct.assert_called_with()

    @patch('d3a_api_client.redis_client_base.RedisClientBase._check_buffer_message_matching_command_and_id')
    def test_function_check_buffer_message_matching_command_and_id_success(self, mock_funct):
        f = mock_funct()
        mock_funct.assert_called_once()

    @patch('d3a_api_client.redis_client_base.RedisClientBase._check_transaction_id_cached_out')
    def test_function_check_transaction_id_cached_out_success(self, mock_funct):
        f = mock_funct()
        mock_funct.assert_called_once()

    @patch('d3a_api_client.redis_client_base.RedisClientBase.register')
    def test_function_register_success(self, mock_funct):
        f = mock_funct()
        mock_funct.assert_called_once()

    @patch('d3a_api_client.redis_client_base.RedisClientBase.unregister')
    def test_function_unregister_success(self, mock_funct):
        f = mock_funct()
        mock_funct.assert_called_once()

    @patch('d3a_api_client.redis_client_base.RedisClientBase._on_register')
    def test_function_on_register_success(self, mock_funct):
        f = mock_funct()
        mock_funct.assert_called_once()

    @patch('d3a_api_client.redis_client_base.RedisClientBase._on_unregister')
    def test_function_on_unregister_success(self, mock_funct):
        f = mock_funct()
        mock_funct.assert_called_once()

    @patch('d3a_api_client.redis_client_base.RedisClientBase._on_unregister')
    def test_function_on_unregister_success(self, mock_funct):
        f = mock_funct()
        mock_funct.assert_called_once()

    @patch('d3a_api_client.redis_client_base.RedisClientBase._on_event_or_response')
    def test_function_on_event_or_response_success(self, mock_funct):
        f = mock_funct()
        mock_funct.assert_called_once()

    @patch('d3a_api_client.redis_client_base.RedisClientBase.select_aggregator')
    def test_function_select_aggregator_success(self, mock_funct):
        f = mock_funct()
        mock_funct.assert_called_once()

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
