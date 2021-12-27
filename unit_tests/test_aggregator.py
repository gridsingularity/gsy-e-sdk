# pylint: disable=missing-function-docstring, protected-access, too-many-public-methods
import uuid
from unittest.mock import patch, PropertyMock, MagicMock
import pytest

from gsy_e_sdk.constants import MAX_WORKER_THREADS

from gsy_e_sdk.utils import get_aggregator_prefix, get_configuration_prefix

from gsy_e_sdk.aggregator import Aggregator

TEST_SIMULATION = {
    "username": "user@test.com",
    "name": "AggregatorTestSuite",
    "uuid": str(uuid.uuid4()),
    "domain_name": "https://cool.webpage.com",
    "websockets_domain_name": "wss://cool.webpage.com/external-ws",
}

TEST_AGGREGATOR_NAME = "TestAggr"
TEST_AGGREGATOR_UUID = str(uuid.uuid4())

TEST_AREA_NAME = "TestArea"
TEST_AREA_MAPPING = {TEST_AREA_NAME: [str(uuid.uuid4())]}

TEST_BATCH_COMMAND_DICT = {"some_device_uuid": [{"command_dict0"},
                                                {"command_dict1"}],
                           "some_other_device_uuid": [{"command_dict2"},
                                                      {"command_dict3"}],
                           }

TEST_BATCH_COMMAND_RESPONSE = {"responses": {"asset_id_1": ["response1", "response2"],
                                             "asset_id_2": ["response3", "response4"]}}


TEST_JWT_KEY_FROM_SERVER = str(uuid.uuid4())
TEST_TRANSACTION_ID = str(uuid.uuid4())


@pytest.fixture(autouse=True, name="mock_connections")
def fixture_mock_connections(mocker):
    """Mock methods and functions which establish external connections."""
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.retrieve_jwt_key_from_server",
                 return_value=TEST_JWT_KEY_FROM_SERVER)
    mocker.patch("gsy_framework.client_connections.utils"
                 ".RepeatingTimer")


@pytest.fixture(name="mock_grid_fee_calculation")
def fixture_mock_grid_fee_calculation(mocker):
    """Mock GridFeeCalculation class."""
    mocker.patch("gsy_e_sdk.aggregator.GridFeeCalculation")


@pytest.fixture(name="mock_environment_use_functions")
def fixture_mock_env_use_functions(mocker):
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.simulation_id_from_env",
                 return_value=TEST_SIMULATION["uuid"])
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.domain_name_from_env",
                 return_value=TEST_SIMULATION["domain_name"])
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.websocket_domain_name_from_env",
                 return_value=TEST_SIMULATION["websockets_domain_name"])


@pytest.fixture(name="mock_outgoing_funcs_in_construction")
def fixture_mock_outgoing_funcs_in_construction(mocker):
    test_aggregators_list = [{"name": TEST_AGGREGATOR_NAME, "uuid": TEST_AGGREGATOR_UUID},
                             {"name": TEST_AGGREGATOR_NAME + "2", "uuid": None}]
    test_new_aggregator_dict = [{"name": TEST_AGGREGATOR_NAME, "uuid": TEST_AGGREGATOR_UUID}]

    mocker.patch("gsy_e_sdk.aggregator.blocking_get_request",
                 return_value=test_aggregators_list)
    mocker.patch("gsy_e_sdk.aggregator.blocking_post_request",
                 return_value=test_new_aggregator_dict)
    mocker.patch("gsy_e_sdk.aggregator.AggregatorWebsocketMessageReceiver")
    mocker.patch("gsy_e_sdk.aggregator.WebsocketThread")
    mocker.patch("gsy_e_sdk.aggregator.ThreadPoolExecutor")


@pytest.fixture(name="mock_execute_batch_command_methods")
def fixture_mock_execute_batch_command_methods(mocker):
    """Mock methods and external functions and other methods used in
    execute_batch_command."""
    mocker.patch("gsy_e_sdk.aggregator.ClientCommandBuffer.buffer_length",
                 new_callable=PropertyMock, return_value=3)
    mocker.patch("gsy_e_sdk.aggregator.ClientCommandBuffer.execute_batch",
                 return_value=TEST_BATCH_COMMAND_DICT)
    mocker.patch("gsy_framework.client_connections.utils.uuid.uuid4",
                 return_value=TEST_TRANSACTION_ID)
    mocker.patch("gsy_framework.client_connections.utils.post_request",
                 return_value=True)


@pytest.fixture(name="aggregator")
def fixture_aggregator(mock_outgoing_funcs_in_construction):  # pylint: disable=unused-argument
    return Aggregator(aggregator_name=TEST_AGGREGATOR_NAME,
                      simulation_id=TEST_SIMULATION["uuid"],
                      domain_name=TEST_SIMULATION["domain_name"],
                      websockets_domain_name=TEST_SIMULATION["websockets_domain_name"]
                      )


class TestAggregator:
    """Test cases for Aggregator class."""

    @staticmethod
    @pytest.mark.usefixtures("mock_outgoing_funcs_in_construction",
                             "mock_environment_use_functions")
    @pytest.mark.parametrize("set_value, expected_set_val",
                             [(TEST_SIMULATION["uuid"], TEST_SIMULATION["uuid"]),
                              (None, TEST_SIMULATION["uuid"])])
    def test_constructor_simulation_id_setup(set_value, expected_set_val):
        # this attribute is inherited from rest_asset_client
        agg = Aggregator(aggregator_name=TEST_AGGREGATOR_NAME,
                         simulation_id=set_value)
        assert agg.simulation_id == expected_set_val

    @staticmethod
    @pytest.mark.usefixtures("mock_outgoing_funcs_in_construction",
                             "mock_environment_use_functions")
    @pytest.mark.parametrize("set_value, expected_set_val",
                             [(TEST_SIMULATION["domain_name"], TEST_SIMULATION["domain_name"]),
                              (None, TEST_SIMULATION["domain_name"])])
    def test_constructor_domain_name_setup(set_value, expected_set_val):
        # this attribute is inherited from rest_asset_client
        agg = Aggregator(aggregator_name=TEST_AGGREGATOR_NAME,
                         domain_name=set_value)
        assert agg.domain_name == expected_set_val

    @staticmethod
    @pytest.mark.usefixtures("mock_outgoing_funcs_in_construction",
                             "mock_environment_use_functions")
    @pytest.mark.parametrize("set_value, expected_set_val",
                             [(TEST_SIMULATION["websockets_domain_name"],
                               TEST_SIMULATION["websockets_domain_name"]),
                              (None, TEST_SIMULATION["websockets_domain_name"])])
    def test_constructor_websockets_domain_name_setup(set_value, expected_set_val):
        # this attribute is inherited from rest_asset_client
        agg = Aggregator(aggregator_name=TEST_AGGREGATOR_NAME,
                         websockets_domain_name=set_value)
        assert agg.websockets_domain_name == expected_set_val

    @staticmethod
    def test_constructor_asset_uuid_setup(aggregator):
        # this attribute is inherited from rest_asset_client
        assert aggregator.asset_uuid == ""

    @staticmethod
    def test_constructor_jwt_token_setup(aggregator):
        # this attribute is inherited from rest_asset_client
        assert aggregator.jwt_token == TEST_JWT_KEY_FROM_SERVER

    @staticmethod
    def test_constructor_aggregator_prefix_setup(aggregator):
        # this attribute is inherited from rest_asset_client
        assert aggregator.aggregator_prefix == get_aggregator_prefix(
            aggregator.domain_name, aggregator.simulation_id)

    @staticmethod
    def test_constructor_configuration_prefix_setup(aggregator):
        # this attribute is inherited from rest_asset_client
        assert aggregator.configuration_prefix == get_configuration_prefix(
            aggregator.domain_name, aggregator.simulation_id)

    @staticmethod
    @pytest.mark.usefixtures("mock_outgoing_funcs_in_construction",
                             "mock_environment_use_functions")
    def test_constructor_grid_fee_calculation_instantiated():
        grid_fee_calc_mock = MagicMock()
        with patch("gsy_e_sdk.aggregator.GridFeeCalculation",
                   return_value=grid_fee_calc_mock) as mocked_class:
            agg = Aggregator(aggregator_name=TEST_AGGREGATOR_NAME)
            mocked_class.assert_called()
            assert agg.grid_fee_calculation is grid_fee_calc_mock

    @staticmethod
    @pytest.mark.usefixtures("mock_outgoing_funcs_in_construction",
                             "mock_environment_use_functions")
    @pytest.mark.parametrize("accept", [True, False])
    def test_constructor_accept_all_devices_setup(accept):
        agg = Aggregator(aggregator_name=TEST_AGGREGATOR_NAME,
                         accept_all_devices=accept)
        assert agg.accept_all_devices == accept

    @staticmethod
    @pytest.mark.usefixtures("mock_outgoing_funcs_in_construction",
                             "mock_environment_use_functions")
    def test_constructor_client_command_buffer_instantiated():
        client_command_buffer_mock = MagicMock()
        with patch("gsy_e_sdk.aggregator.ClientCommandBuffer",
                   return_value=client_command_buffer_mock) as mocked_class:
            agg = Aggregator(aggregator_name=TEST_AGGREGATOR_NAME)
            mocked_class.assert_called()
            assert agg._client_command_buffer is client_command_buffer_mock

    @staticmethod
    def test_constructor_latest_grid_tree_setup(aggregator):
        assert aggregator.latest_grid_tree == {}

    @staticmethod
    def test_constructor_latest_grid_tree_flat_setup(aggregator):
        assert aggregator.latest_grid_tree_flat == {}

    @staticmethod
    def test_constructor_area_name_uuid_mapping_setup(aggregator):
        assert aggregator.area_name_uuid_mapping == {}

    @staticmethod
    def test_start_websocket_connection_dispatcher_instantiated(aggregator):
        agg_websocket_msg_receiver_mock = MagicMock()
        with patch("gsy_e_sdk.aggregator.AggregatorWebsocketMessageReceiver",
                   return_value=agg_websocket_msg_receiver_mock) as mocked_class:
            aggregator.start_websocket_connection()

            mocked_class.assert_called_once()
            assert aggregator.dispatcher is agg_websocket_msg_receiver_mock

    @staticmethod
    def test_start_websocket_connection_websocket_instantiated(aggregator):
        websocket_thread_mock = MagicMock()
        with patch("gsy_e_sdk.aggregator.WebsocketThread",
                   return_value=websocket_thread_mock) as mocked_class:
            aggregator.start_websocket_connection()
            websocket_uri = f"{aggregator.websockets_domain_name}/" \
                            f"{aggregator.simulation_id}/aggregator/" \
                            f"{aggregator.aggregator_uuid}/"

            mocked_class.assert_called_with(websocket_uri, aggregator.domain_name,
                                            aggregator.dispatcher)
            assert aggregator.websocket_thread is websocket_thread_mock

    @staticmethod
    def test_start_websocket_connection_websocket_thread_start(aggregator):
        aggregator.websocket_thread.start.assert_called_once()

    @staticmethod
    def test_start_websocket_connection_callback_thread_instantiated(aggregator):
        thread_pool_executor_mock = MagicMock()
        with patch("gsy_e_sdk.aggregator.ThreadPoolExecutor",
                   return_value=thread_pool_executor_mock) as mocked_class:
            aggregator.start_websocket_connection()

            mocked_class.assert_called_with(max_workers=MAX_WORKER_THREADS)
            assert aggregator.callback_thread is thread_pool_executor_mock

    @staticmethod
    @pytest.mark.usefixtures("mock_grid_fee_calculation")
    def test_calculate_grid_fee_called_with_args(aggregator):
        aggregator.calculate_grid_fee("start_market",
                                      "target_market",
                                      "current_market_fee")
        aggregator.grid_fee_calculation.calculate_grid_fee.assert_called_with(
            "start_market",
            "target_market",
            "current_market_fee")

    @staticmethod
    @pytest.mark.usefixtures("mock_grid_fee_calculation")
    def test_calculate_grid_fee_returns_expected(aggregator):
        aggregator.grid_fee_calculation.calculate_grid_fee.return_value = "TEST_OK"
        ret_val = aggregator.calculate_grid_fee("start_market",
                                                "target_market",
                                                "current_market_fee")
        assert ret_val == "TEST_OK"

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods")
    def test_execute_batch_commands_commands_buffer_length_with_length_zero(aggregator):
        with patch("gsy_e_sdk.aggregator.ClientCommandBuffer.buffer_length",
                   new_callable=PropertyMock, return_value=0):
            assert aggregator.execute_batch_commands() is None

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods")
    def test_execute_batch_commands_execute_batch_called(aggregator):
        with patch("gsy_e_sdk.aggregator.ClientCommandBuffer.execute_batch"
                   ) as mocked_method:
            aggregator.device_uuid_list = TEST_BATCH_COMMAND_DICT.keys()
            aggregator.execute_batch_commands()
            mocked_method.assert_called()

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods")
    def test_execute_batch_commands_not_all_uuids_in_selected_device_uuid_list_raise_exception(
            aggregator):
        with pytest.raises(Exception):
            aggregator.execute_batch_commands()

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods")
    def test_execute_batch_commands_post_request_called_with_args(aggregator):
        data = {"aggregator_uuid": TEST_AGGREGATOR_UUID,
                "batch_commands": TEST_BATCH_COMMAND_DICT,
                "transaction_id": TEST_TRANSACTION_ID}
        endpoint = f"{aggregator.aggregator_prefix}batch-commands/"
        aggregator.device_uuid_list = TEST_BATCH_COMMAND_DICT.keys()

        with patch("gsy_framework.client_connections.utils.post_request",
                   return_value=True) as mocked_func:
            aggregator.execute_batch_commands()
            mocked_func.assert_called_with(endpoint, data, TEST_JWT_KEY_FROM_SERVER)

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods")
    def test_execute_batch_commands_post_request_not_posted_return_none(
            aggregator):
        with patch("gsy_framework.client_connections.utils.post_request",
                   return_value=None):
            aggregator.device_uuid_list = TEST_BATCH_COMMAND_DICT.keys()
            assert aggregator.execute_batch_commands() is None

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods")
    def test_execute_batch_commands_clear_command_buffer_called(aggregator):
        with patch("gsy_e_sdk.aggregator.ClientCommandBuffer.clear"
                   ) as mocked_method:
            aggregator.device_uuid_list = TEST_BATCH_COMMAND_DICT.keys()
            aggregator.execute_batch_commands()
            mocked_method.assert_called_once()

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods")
    def test_execute_batch_commands_wait_for_command_response_called(aggregator):
        aggregator.device_uuid_list = TEST_BATCH_COMMAND_DICT.keys()
        aggregator.execute_batch_commands()
        aggregator.dispatcher.wait_for_command_response.assert_called_once()

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods")
    def test_execute_batch_commands_returns_response(aggregator):
        aggregator.device_uuid_list = TEST_BATCH_COMMAND_DICT.keys()
        aggregator.dispatcher.wait_for_command_response.return_value = \
            TEST_BATCH_COMMAND_RESPONSE
        assert aggregator.execute_batch_commands() == TEST_BATCH_COMMAND_RESPONSE

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods")
    def test_execute_batch_commands_log_bid_offer_confirmation_called(aggregator):
        with patch("gsy_e_sdk.aggregator.log_bid_offer_confirmation") as mocked_method:
            aggregator.device_uuid_list = TEST_BATCH_COMMAND_DICT.keys()
            aggregator.dispatcher.wait_for_command_response.return_value = \
                TEST_BATCH_COMMAND_RESPONSE
            aggregator.execute_batch_commands()
            mocked_method.assert_called()

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods")
    def test_execute_batch_commands_log_deleted_bid_offer_confirmation_called(
            aggregator):
        with patch("gsy_e_sdk.aggregator.log_deleted_bid_offer_confirmation"
                   ) as mocked_method:
            aggregator.device_uuid_list = TEST_BATCH_COMMAND_DICT.keys()
            aggregator.dispatcher.wait_for_command_response.return_value = \
                TEST_BATCH_COMMAND_RESPONSE
            aggregator.execute_batch_commands()
            mocked_method.assert_called()

    @staticmethod
    @pytest.mark.parametrize("uuid_mapping, expected_ret_val",
                             [(TEST_AREA_MAPPING, TEST_AREA_MAPPING[TEST_AREA_NAME][0]),
                              ({}, None)])
    def test_get_uuid_from_area_name_return_expected(uuid_mapping, expected_ret_val,
                                                     aggregator):
        aggregator.area_name_uuid_mapping = uuid_mapping
        with patch("gsy_e_sdk.aggregator.get_uuid_from_area_name_in_tree_dict",
                   return_value=TEST_AREA_MAPPING[TEST_AREA_NAME][0]):
            ret_val = aggregator.get_uuid_from_area_name(TEST_AREA_NAME)
            assert ret_val is expected_ret_val

    @staticmethod
    def test_get_uuid_from_area_name_call_expected(aggregator):
        aggregator.area_name_uuid_mapping = TEST_AREA_MAPPING
        with patch("gsy_e_sdk.aggregator.get_uuid_from_area_name_in_tree_dict",
                   return_value=TEST_AREA_MAPPING[TEST_AREA_NAME][0]) as mocked_func:
            aggregator.get_uuid_from_area_name(TEST_AREA_NAME)
            mocked_func.assert_called_with(aggregator.area_name_uuid_mapping, TEST_AREA_NAME)

    @staticmethod
    @pytest.mark.parametrize("blocking_get_request_ret_val, expected_ret_val",
                             [([TEST_AGGREGATOR_NAME], [TEST_AGGREGATOR_NAME]), (None, [])])
    def test_list_aggregators_request_empty_and_not_empty_list(blocking_get_request_ret_val,
                                                               expected_ret_val,
                                                               aggregator):
        with patch("gsy_e_sdk.aggregator.blocking_get_request",
                   return_value=blocking_get_request_ret_val):
            aggs_list = aggregator.list_aggregators()
            assert aggs_list == expected_ret_val

    @staticmethod
    def test_get_configuration_registry_call_blocking_get_request(aggregator):
        conf = f"{aggregator.configuration_prefix}registry"
        with patch("gsy_e_sdk.aggregator.blocking_get_request") as mocked_func:
            aggregator.get_configuration_registry()
            mocked_func.assert_called_with(conf, {}, TEST_JWT_KEY_FROM_SERVER)

    @staticmethod
    def test_delete_aggregator_calls_blocking_post_request(aggregator):
        conf = f"{aggregator.aggregator_prefix}delete-aggregator/"
        with patch("gsy_e_sdk.aggregator.blocking_post_request") as mocked_func:
            aggregator.delete_aggregator()
            mocked_func.assert_called_with(
                conf,
                {"aggregator_uuid": aggregator.aggregator_uuid},
                aggregator.jwt_token
            )

    @staticmethod
    @pytest.mark.usefixtures("mock_outgoing_funcs_in_construction",
                             "mock_environment_use_functions")
    def test_add_to_batch_commands_returns_client_command_buffer():
        client_command_buffer_mock = MagicMock()
        with patch("gsy_e_sdk.aggregator.ClientCommandBuffer",
                   return_value=client_command_buffer_mock):
            agg = Aggregator(aggregator_name=TEST_AGGREGATOR_NAME)
            buffer_instance = agg.add_to_batch_commands
            assert buffer_instance == client_command_buffer_mock

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods")
    def test_commands_buffer_length_returns_expected_buffer_length(aggregator):
        buffer_length_mock = MagicMock()
        with patch("gsy_e_sdk.aggregator.ClientCommandBuffer.buffer_length",
                   new_callable=PropertyMock,
                   return_value=buffer_length_mock):
            assert aggregator.commands_buffer_length == buffer_length_mock
