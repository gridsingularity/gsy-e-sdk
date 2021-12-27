# pylint: disable=missing-function-docstring, protected-access, no-member
import json
import uuid
from unittest.mock import patch, PropertyMock, MagicMock, call
import pytest
from gsy_e_sdk.constants import LOCAL_REDIS_URL

from gsy_e_sdk.redis_aggregator import RedisAggregator, RedisAggregatorAPIException

TEST_AGGREGATOR_NAME = "TestAgg"

TEST_TRANSACTION_ID = str(uuid.uuid4())
TEST_AGGREGATOR_UUID = str(uuid.uuid4())

TEST_DEVICE_UUID_1 = str(uuid.uuid4())
TEST_DEVICE_UUID_2 = str(uuid.uuid4())

TEST_BATCH_COMMAND_DICT = {
    TEST_DEVICE_UUID_1: ["TEST_BATCH_COMMAND"],
    TEST_DEVICE_UUID_2: ["TEST_BATCH_COMMAND"]
}


TEST_RESPONSE = {"status": "ready",
                 "command": "offer",
                 "offer": {"energy": 1,
                           "price": 1,
                           "seller": "test_seller"},
                 "bid": {"energy": 1,
                         "price": 1,
                         "buyer": "test_buyer"},
                 }


@pytest.fixture(autouse=True)
def fixture_mock_connections(mocker):
    mocker.patch("gsy_e_sdk.redis_aggregator.StrictRedis")
    mocker.patch("gsy_e_sdk.redis_aggregator.ThreadPoolExecutor")


@pytest.fixture(name="mock_grid_fee_calculation")
def fixture_mock_grid_fee_calculation(mocker):
    """Mock GridFeeCalculation class."""
    mocker.patch("gsy_e_sdk.redis_aggregator.GridFeeCalculation")


@pytest.fixture(name="mock_transaction_id_and_timeout_blocking")
def fixture_mock_transaction_id_and_timeout_blocking(mocker):
    mocker.patch("gsy_e_sdk.redis_aggregator.uuid.uuid4",
                 return_value=TEST_TRANSACTION_ID)
    mocker.patch("gsy_e_sdk.redis_aggregator.wait_until_timeout_blocking")


@pytest.fixture(name="mock_client_command_buffer_attributes")
def fixture_mock_client_command_buffer(mocker):
    mocker.patch("gsy_e_sdk.redis_aggregator.ClientCommandBuffer.buffer_length",
                 new_callable=PropertyMock, return_value=3)
    mocker.patch("gsy_e_sdk.redis_aggregator.ClientCommandBuffer.execute_batch",
                 return_value=TEST_BATCH_COMMAND_DICT)


@pytest.fixture(name="aggregator")
def fixture_aggregator(mocker):
    mocker.patch("gsy_e_sdk.redis_aggregator.uuid.uuid4",
                 return_value=TEST_TRANSACTION_ID)
    mocker.patch("gsy_e_sdk.redis_aggregator.wait_until_timeout_blocking")
    return RedisAggregator(aggregator_name=TEST_AGGREGATOR_NAME)


class TestRedisAggregatorConstructor:
    """Include tests for the constructor of the RedisAggregator class."""

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id_and_timeout_blocking")
    def test_constructor_grid_fee_calculation_instantiated():
        grid_fee_calculation_mock = MagicMock()
        with patch("gsy_e_sdk.redis_aggregator.GridFeeCalculation",
                   return_value=grid_fee_calculation_mock):
            agg = RedisAggregator(aggregator_name=TEST_AGGREGATOR_NAME)
            assert agg.grid_fee_calculation is grid_fee_calculation_mock

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id_and_timeout_blocking")
    def test_constructor_redis_db_instantiated():
        redis_db_mock = MagicMock()
        with patch("gsy_e_sdk.redis_aggregator.StrictRedis.from_url",
                   return_value=redis_db_mock) as redis_from_url:
            agg = RedisAggregator(aggregator_name=TEST_AGGREGATOR_NAME)
            redis_from_url.assert_called_with(LOCAL_REDIS_URL)
            assert agg.redis_db is redis_db_mock

    @staticmethod
    def test_constructor_pubsub_instantiated(aggregator):
        aggregator.redis_db.pubsub.assert_called()
        assert aggregator.pubsub is aggregator.redis_db.pubsub()

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id_and_timeout_blocking")
    def test_constructor_client_command_buffer_instantiated():
        client_command_buffer_mock = MagicMock()
        with patch("gsy_e_sdk.redis_aggregator.ClientCommandBuffer",
                   return_value=client_command_buffer_mock):
            agg = RedisAggregator(aggregator_name=TEST_AGGREGATOR_NAME)
        assert agg._client_command_buffer is client_command_buffer_mock

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id_and_timeout_blocking")
    def test_constructor_threadpoolexecutor_instantiated():
        thread_pool_executor_mock = MagicMock
        with patch("gsy_e_sdk.redis_aggregator.ThreadPoolExecutor",
                   return_value=thread_pool_executor_mock) as mocked_class:
            agg = RedisAggregator(aggregator_name=TEST_AGGREGATOR_NAME)
            mocked_class.assert_called()
        assert agg.executor is thread_pool_executor_mock

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id_and_timeout_blocking")
    def test_constructor_connect_and_subscribe_side_effects_1():
        """Test side effects due to the methods called with _connect_and_subscribe().
        -> Side effects of _subscribe_to_aggregator_response_and_start_redis_thread
        """

        aggregator = RedisAggregator(aggregator_name=TEST_AGGREGATOR_NAME)
        channel_dict_1 = {"aggregator_response": aggregator._aggregator_response_callback}

        aggregator.pubsub.run_in_thread.assert_called_with(daemon=True)
        aggregator.pubsub.psubscribe.assert_has_calls([call(**channel_dict_1)])

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id_and_timeout_blocking")
    def test_constructor_connect_and_subscribe_side_effects_2():
        """Test side effects due to the methods called with _connect_and_subscribe().
        -> Side effects of _connect_to_simulation -> _create_aggregator private method.
        """
        aggregator = RedisAggregator(aggregator_name=TEST_AGGREGATOR_NAME)
        data = {"name": aggregator.aggregator_name, "type": "CREATE",
                "transaction_id": TEST_TRANSACTION_ID}

        aggregator.redis_db.publish.assert_called_with("aggregator", json.dumps(data))
        assert TEST_TRANSACTION_ID in aggregator._transaction_id_buffer
        assert aggregator.aggregator_uuid is TEST_TRANSACTION_ID

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id_and_timeout_blocking")
    def test_constructor_connect_and_subscribe_side_effects_3():
        """Test side effects due to the methods called with _connect_and_subscribe().
        -> Side effects of _subscribe_to_response_channels private method
        """
        aggregator = RedisAggregator(aggregator_name=TEST_AGGREGATOR_NAME)
        channel_dict_2 = {f"external-aggregator/*/{aggregator.aggregator_uuid}/events"
                          f"/all": aggregator._events_callback_dict,
                          f"external-aggregator/*/{aggregator.aggregator_uuid}/response"
                          f"/batch_commands": aggregator._batch_response,
                          }

        aggregator.pubsub.psubscribe.assert_called_with(**channel_dict_2)

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id_and_timeout_blocking")
    def test_constructor_lock_instantiated():
        lock_mock = MagicMock()
        with patch("gsy_e_sdk.redis_aggregator.Lock",
                   return_value=lock_mock) as mocked_class:
            agg = RedisAggregator(aggregator_name=TEST_AGGREGATOR_NAME)
            mocked_class.assert_called()
            assert agg.lock is lock_mock


@pytest.mark.usefixtures("mock_transaction_id_and_timeout_blocking")
class TestRedisAggregatorDeleteAggregator:
    """Test cases for RedisAggregator's delete_aggregator method."""

    @staticmethod
    def test_delete_aggregator_redis_db_publishes_data(aggregator):
        data = {"name": TEST_AGGREGATOR_NAME,
                "aggregator_uuid": TEST_AGGREGATOR_UUID,
                "type": "DELETE",
                "transaction_id": TEST_TRANSACTION_ID}

        aggregator.aggregator_uuid = TEST_AGGREGATOR_UUID

        aggregator.delete_aggregator()
        aggregator.redis_db.publish.assert_called_with(
            "aggregator", json.dumps(data))

    @staticmethod
    def test_delete_aggregator_api_time_out_raise_exception(aggregator):
        with patch("gsy_e_sdk.redis_aggregator.wait_until_timeout_blocking",
                   side_effect=AssertionError):
            aggregator.aggregator_uuid = TEST_AGGREGATOR_UUID
            with pytest.raises(RedisAggregatorAPIException):
                aggregator.delete_aggregator()

    @staticmethod
    @pytest.mark.parametrize("test_is_blocking, expected_ret_val",
                             [(True, TEST_TRANSACTION_ID), (False, None)])
    def test_delete_aggregator_returns_expected(aggregator, test_is_blocking,
                                                expected_ret_val):
        aggregator.aggregator_uuid = TEST_AGGREGATOR_UUID
        assert aggregator.delete_aggregator(is_blocking=test_is_blocking) is expected_ret_val


@pytest.mark.usefixtures("mock_client_command_buffer_attributes")
class TestRedisAggregatorExecuteBatchCommands:
    """Test cases for RedisAggregator's execute_batch_commands method."""

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id_and_timeout_blocking")
    def test_execute_batch_commands_commands_buffer_length_zero_return_none():
        with patch("gsy_e_sdk.redis_aggregator.RedisAggregator.commands_buffer_length",
                   new_callable=PropertyMock,
                   return_value=0):
            agg = RedisAggregator(aggregator_name=TEST_AGGREGATOR_NAME)
            assert agg.execute_batch_commands() is None

    @staticmethod
    def test_execute_batch_commands_not_all_uuids_in_selected_device_uuid_list_raise_exception(
            aggregator):
        aggregator.device_uuid_list = [TEST_DEVICE_UUID_1]

        with pytest.raises(Exception):
            aggregator.execute_batch_commands(is_blocking=False)

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id_and_timeout_blocking")
    def test_execute_batch_commands_batched_command_published(aggregator):
        aggregator.device_uuid_list = [TEST_DEVICE_UUID_1, TEST_DEVICE_UUID_2]
        aggregator.aggregator_uuid = TEST_AGGREGATOR_UUID

        batched_command_input = {"type": "BATCHED", "transaction_id": TEST_TRANSACTION_ID,
                                 "aggregator_uuid": TEST_AGGREGATOR_UUID,
                                 "batch_commands": TEST_BATCH_COMMAND_DICT}
        batched_channel_input = f"external//aggregator/{TEST_AGGREGATOR_UUID}/batch_commands"

        aggregator.execute_batch_commands(is_blocking=False)
        aggregator.redis_db.publish.assert_called_with(batched_channel_input,
                                                       json.dumps(batched_command_input))

    @staticmethod
    def test_execute_batch_commands_api_time_out_raise_exception(aggregator):
        with patch("gsy_e_sdk.redis_aggregator.wait_until_timeout_blocking",
                   side_effect=AssertionError):
            aggregator.device_uuid_list = [TEST_DEVICE_UUID_1, TEST_DEVICE_UUID_2]
            aggregator.aggregator_uuid = TEST_AGGREGATOR_UUID
            with pytest.raises(RedisAggregatorAPIException):
                aggregator.execute_batch_commands()

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id_and_timeout_blocking")
    @pytest.mark.parametrize("is_blocking, trans_id_resp_buffer, expected_ret_val",
                             [(True, {TEST_TRANSACTION_ID: TEST_RESPONSE}, TEST_RESPONSE),
                              (False, {TEST_TRANSACTION_ID: TEST_RESPONSE}, None),
                              (True, {}, None)])
    def test_execute_batch_commands_returns_expected(aggregator, is_blocking,
                                                     trans_id_resp_buffer, expected_ret_val):
        aggregator.device_uuid_list = [TEST_DEVICE_UUID_1, TEST_DEVICE_UUID_2]
        aggregator.aggregator_uuid = TEST_AGGREGATOR_UUID
        aggregator._transaction_id_response_buffer = trans_id_resp_buffer

        assert aggregator.execute_batch_commands(is_blocking=is_blocking) is expected_ret_val


@pytest.mark.usefixtures("mock_grid_fee_calculation")
class TestRedisAggregatorCalculateGridFee:
    """Test cases for RedisAggregator's calculate_grid_fee method."""

    @staticmethod
    def test_calculate_grid_fee_called_with_args(aggregator):
        aggregator.calculate_grid_fee("start_market",
                                      "target_market",
                                      "current_market_fee")
        aggregator.grid_fee_calculation.calculate_grid_fee.assert_called_with(
            "start_market",
            "target_market",
            "current_market_fee")

    @staticmethod
    def test_calculate_grid_fee_returns_expected(aggregator):
        aggregator.grid_fee_calculation.calculate_grid_fee.return_value = "TEST_OK"
        ret_val = aggregator.calculate_grid_fee("start_market",
                                                "target_market",
                                                "current_market_fee")
        assert ret_val == "TEST_OK"


def test_get_uuid_from_area_name_returns_expected(aggregator):
    test_area_mapping = {"TestArea": [str(uuid.uuid4())]}

    aggregator.area_name_uuid_mapping = test_area_mapping
    area_name = "TestArea"
    ret_val = test_area_mapping[area_name][0]

    assert aggregator.get_uuid_from_area_name(area_name) == ret_val


@pytest.mark.usefixtures("mock_transaction_id_and_timeout_blocking")
def test_add_to_batch_commands_returns_client_command_buffer():
    client_command_buffer_mock = MagicMock()
    with patch("gsy_e_sdk.redis_aggregator.ClientCommandBuffer",
               return_value=client_command_buffer_mock):
        aggregator = RedisAggregator(aggregator_name=TEST_AGGREGATOR_NAME)
        assert aggregator.add_to_batch_commands is client_command_buffer_mock


@pytest.mark.usefixtures("mock_client_command_buffer_attributes")
def test_commands_buffer_length_returns_expected_buffer_length(aggregator):
    assert aggregator.commands_buffer_length == 3
