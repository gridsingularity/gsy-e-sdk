# pylint: disable=missing-function-docstring, protected-access
import json
import uuid
from unittest.mock import patch, PropertyMock, MagicMock
import pytest

from gsy_e_sdk.redis_aggregator import RedisAggregator, RedisAggregatorAPIException

TEST_AGGREGATOR_NAME = "TestAgg"

TEST_AREA_NAME = "TestArea"
TEST_AREA_MAPPING = {TEST_AREA_NAME: [str(uuid.uuid4())]}

TEST_TRANSACTION_ID = str(uuid.uuid4())
TEST_AGGREGATOR_UUID = str(uuid.uuid4())

TEST_DEVICE_UUID_1 = str(uuid.uuid4())
TEST_DEVICE_UUID_2 = str(uuid.uuid4())
TEST_BATCH_COMMAND = "TEST_BATCH_COMMAND"

TEST_BATCH_COMMAND_DICT = {
    TEST_DEVICE_UUID_1: [TEST_BATCH_COMMAND],
    TEST_DEVICE_UUID_2: [TEST_BATCH_COMMAND]
}

TEST_TRANSACTION_ID_RESPONSE = "TEST_TRANSACTION_ID_RESPONSE"
TEST_TRANSACTION_ID_BUFFER = {
    TEST_TRANSACTION_ID: {"transaction_id": TEST_TRANSACTION_ID,
                          "response": TEST_TRANSACTION_ID_RESPONSE}
}

TEST_RESPONSE = "TEST_RESPONSE"
TEST_TRANSACTION_ID_RESPONSE_BUFFER = {TEST_TRANSACTION_ID: TEST_RESPONSE}


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
    mocker.patch("gsy_e_sdk.redis_aggregator.RedisAggregator._create_aggregator")
    return RedisAggregator(aggregator_name=TEST_AGGREGATOR_NAME)


class TestRedisAggregatorConstructor:
    """Includes tests for the constructor of the RedisAggregator class."""

    @staticmethod
    @pytest.mark.usefixtures("mock_grid_fee_calculation")
    def test_constructor_grid_fee_calculation_instantiated(aggregator):
        assert aggregator.grid_fee_calculation is not None

    @staticmethod
    def test_constructor_redis_db_instantiated(aggregator):
        assert aggregator.redis_db is not None

    @staticmethod
    def test_constructor_pubsub_instantiated(aggregator):
        assert aggregator.pubsub is not None

    @staticmethod
    def test_constructor_client_command_buffer_instantiated(aggregator):
        assert aggregator._client_command_buffer is not None

    @staticmethod
    def test_constructor_threadpoolexecutor_instantiated(aggregator):
        assert aggregator.executor is not None

    @staticmethod
    def test_constructor_connect_and_subscribe_called():
        with patch("gsy_e_sdk.redis_aggregator."
                   "RedisAggregator._connect_and_subscribe") as mocked_method:
            RedisAggregator(aggregator_name=TEST_AGGREGATOR_NAME)
            mocked_method.assert_called()

    @staticmethod
    def test_constructor_lock_instantiated(aggregator):
        assert aggregator.lock is not None


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
    def test_execute_batch_commands_commands_buffer_length_zero_return_none(aggregator):
        with patch("gsy_e_sdk.redis_aggregator.ClientCommandBuffer.buffer_length",
                   new_callable=PropertyMock, return_value=0):
            assert aggregator.execute_batch_commands() is None

    @staticmethod
    def test_execute_batch_commands_not_all_uuids_in_selected_device_uuid_list_raise_exception(
            aggregator):
        aggregator.device_uuid_list = [TEST_DEVICE_UUID_1]

        with pytest.raises(Exception):
            aggregator.execute_batch_commands(is_blocking=False)

    @staticmethod
    def test_execute_batch_commands_batched_command_published(aggregator):
        uuid.uuid4 = MagicMock(return_value=TEST_TRANSACTION_ID)

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
    @pytest.mark.usefixtures("mock_transaction_id_and_timeout_blocking")
    def test_execute_batch_commands_api_time_out_raise_exception(aggregator):
        with patch("gsy_e_sdk.redis_aggregator.wait_until_timeout_blocking",
                   side_effect=AssertionError):
            aggregator.device_uuid_list = [TEST_DEVICE_UUID_1, TEST_DEVICE_UUID_2]
            aggregator.aggregator_uuid = TEST_AGGREGATOR_UUID
            with pytest.raises(RedisAggregatorAPIException):
                aggregator.execute_batch_commands()

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id_and_timeout_blocking")
    @pytest.mark.parametrize("is_blocking_, trans_id_resp_buffer, expected_ret_val",
                             [(True, TEST_TRANSACTION_ID_RESPONSE_BUFFER, TEST_RESPONSE),
                              (False, TEST_TRANSACTION_ID_RESPONSE_BUFFER, None),
                              (True, {}, None)])
    def test_execute_batch_commands_returns_expected(aggregator, is_blocking_,
                                                     trans_id_resp_buffer, expected_ret_val):
        aggregator.device_uuid_list = [TEST_DEVICE_UUID_1, TEST_DEVICE_UUID_2]
        aggregator.aggregator_uuid = TEST_AGGREGATOR_UUID
        aggregator._transaction_id_response_buffer = trans_id_resp_buffer

        assert aggregator.execute_batch_commands(is_blocking=is_blocking_) is expected_ret_val


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
    aggregator.area_name_uuid_mapping = TEST_AREA_MAPPING
    area_name = TEST_AREA_NAME
    ret_val = TEST_AREA_MAPPING[TEST_AREA_NAME][0]

    patch("gsy_e_sdk.redis_aggregator.get_uuid_from_area_name_in_tree_dict",
          return_value=ret_val)
    assert aggregator.get_uuid_from_area_name(area_name) == ret_val


def test_add_to_batch_commands_returns_client_command_buffer(aggregator):
    buffer_instance = aggregator.add_to_batch_commands
    assert buffer_instance == aggregator._client_command_buffer


@pytest.mark.usefixtures("mock_client_command_buffer_attributes")
def test_commands_buffer_length_returns_expected_buffer_length(aggregator):
    assert aggregator.commands_buffer_length == 3
