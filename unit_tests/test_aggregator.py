# pylint: disable=missing-function-docstring, protected-access
import uuid
from unittest.mock import patch, PropertyMock, MagicMock, call
import pytest
from gsy_e_sdk.utils import get_aggregator_prefix, get_configuration_prefix

from gsy_e_sdk.aggregator import Aggregator

TEST_SIMULATION = {
    "username": "user@test.com",
    "name": "AggregatorTestSuite",
    "uuid": "some_random_uuid",
    "domain_name": "https://cool.webpage.com",
    "websockets_domain_name": "wss://cool.webpage.com/external-ws",
}

TEST_AGGREGATOR_NAME = "TestAggr"
TEST_AGGREGATOR_UUID = str(uuid.uuid4())

TEST_AGGREGATORS_LIST = [{"name": TEST_AGGREGATOR_NAME, "uuid": TEST_AGGREGATOR_UUID},
                         {"name": TEST_AGGREGATOR_NAME + "2", "uuid": None}]

TEST_NEW_AGGREGATOR_DICT = [{"name": TEST_AGGREGATOR_NAME, "uuid": TEST_AGGREGATOR_UUID}]

TEST_AREA_NAME = "TestArea"
TEST_AREA_MAPPING = {TEST_AREA_NAME: [str(uuid.uuid4())]}

TEST_BATCH_COMMAND_DICT = {"some_device_uuid": [{"command_dict0"},
                                                {"command_dict1"}],
                           "some_other_device_uuid": [{"command_dict2"},
                                                      {"command_dict3"}],
                           }

TEST_BATCH_COMMAND_RESPONSE = {"responses": {"asset_id_1": ["response1", "response2"],
                                             "asset_id_2": ["response3", "response4"]}}

TEST_CONFIGURATION_PREFIX = "/configuration_prefix/"

TEST_AGGREGATOR_PREFIX = "/aggregator_prefix/"

TEST_JWT_KEY_FROM_SERVER = str(uuid.uuid4())
TEST_TRANSACTION_ID = str(uuid.uuid4())


@pytest.fixture(autouse=True, name="mock_connections")
def fixture_mock_connections(mocker):
    """Mock methods and functions which establish external connections."""
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.retrieve_jwt_key_from_server",
                 return_value=TEST_JWT_KEY_FROM_SERVER)
    mocker.patch("gsy_framework.client_connections.utils"
                 ".RepeatingTimer")


@pytest.fixture(name="mock_prefixes")
def fixture_mock_prefixes(mocker):
    """Mock external prefix calculation functions."""
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.get_aggregator_prefix",
                 return_value=TEST_AGGREGATOR_PREFIX, autospec=True)
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.get_configuration_prefix",
                 return_value=TEST_CONFIGURATION_PREFIX)


@pytest.fixture(name="mock_start_websocket_funcs")
def fixture_mock_start_websocket_funcs(mocker):
    """Mock external classes used in Aggregator.start_websocket_connections."""
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
    mocker.patch("gsy_e_sdk.aggregator.ClientCommandBuffer.clear")
    mocker.patch("gsy_e_sdk.aggregator.Aggregator._post_request",
                 return_value=[TEST_TRANSACTION_ID, True])
    mocker.patch("gsy_e_sdk.aggregator.Aggregator."
                 "_all_uuids_in_selected_device_uuid_list",
                 return_value=True)
    mocker.patch("gsy_e_sdk.aggregator.log_bid_offer_confirmation")
    mocker.patch("gsy_e_sdk.aggregator.log_deleted_bid_offer_confirmation")


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
    # patch for list_aggregators() method called in
    # constructor -> _connect_to_simulation.
    mocker.patch("gsy_e_sdk.aggregator.blocking_get_request",
                 return_value=TEST_AGGREGATORS_LIST)

    # patch for _create_agregator() p-method called in
    # constructor -> _connect_to_simulation.
    mocker.patch("gsy_e_sdk.aggregator.blocking_post_request",
                 return_value=TEST_NEW_AGGREGATOR_DICT)

    # patches for start_websocket_connection() method called in
    # constructor -> _connect_to_simulation.
    mocker.patch("gsy_e_sdk.aggregator.AggregatorWebsocketMessageReceiver")
    mocker.patch("gsy_e_sdk.aggregator.WebsocketThread")
    mocker.patch("gsy_e_sdk.aggregator.ThreadPoolExecutor")


@pytest.fixture(name="aggregator")
def fixture_aggregator(mock_outgoing_funcs_in_construction):  # pylint: disable=unused-argument
    return Aggregator(aggregator_name=TEST_AGGREGATOR_NAME,
                      simulation_id=TEST_SIMULATION["uuid"],
                      domain_name=TEST_SIMULATION["domain_name"],
                      websockets_domain_name=TEST_SIMULATION["websockets_domain_name"]
                      )


class TestAggregatorConstructor:
    """Test cases for Aggregator's class constructor."""

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
    def _test_constructor__connect_to_simulation_called(aggregator):
        # this feature is no longer tested because it is a private method
        # and the classes which are invoked due to it are public and are
        # also under test in this test suite.
        pass

    @staticmethod
    def test_constructor_latest_grid_tree_setup(aggregator):
        assert aggregator.latest_grid_tree == {}

    @staticmethod
    def test_constructor_latest_grid_tree_flat_setup(aggregator):
        assert aggregator.latest_grid_tree_flat == {}

    @staticmethod
    def test_constructor_area_name_uuid_mapping_setup(aggregator):
        assert aggregator.area_name_uuid_mapping == {}


class TestAggregatorStartWebsocketConnection:
    """Test cases for Aggregator's start_websocket_connection method."""

    @staticmethod
    @pytest.mark.usefixtures("mock_start_websocket_funcs")
    def test_start_websocket_connection_dispatcher_instantiated(aggregator):
        aggregator.start_websocket_connection()
        assert aggregator.dispatcher is not None

    @staticmethod
    @pytest.mark.usefixtures("mock_start_websocket_funcs")
    def test_start_websocket_connection_websocket_instantiated(aggregator):
        aggregator.aggregator_uuid = TEST_AGGREGATOR_UUID
        aggregator.start_websocket_connection()
        assert aggregator.websocket_thread is not None

    @staticmethod
    # @pytest.mark.usefixtures("mock_start_websocket_funcs")
    def test_start_websocket_connection_websocket_thread_start(aggregator):
        aggregator.start_websocket_connection()
        aggregator.websocket_thread.start.assert_has_calls([call(), call()])

    @staticmethod
    @pytest.mark.usefixtures("mock_start_websocket_funcs")
    def test_start_websocket_connection_callback_thread_instantiated(aggregator):
        aggregator.start_websocket_connection()
        assert aggregator.callback_thread is not None


class TestAggregatorCalculateGridFee:
    """Test cases for Aggregator's calculate_grid_fee method."""

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


class TestAggregatorExecuteBatchCommands:
    """Test cases for Aggregator's execute_batch_commands method."""

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods")
    def test_execute_batch_commands_commands_buffer_length_with_length_zero(aggregator):
        with patch("gsy_e_sdk.aggregator.ClientCommandBuffer.buffer_length",
                   new_callable=PropertyMock, return_value=0):
            assert aggregator.execute_batch_commands() is None

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods", "mock_start_websocket_funcs")
    def test_execute_batch_commands_execute_batch_called(aggregator):
        aggregator.start_websocket_connection()
        aggregator.execute_batch_commands()
        aggregator._client_command_buffer.execute_batch.assert_called_once()

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods", "mock_start_websocket_funcs")
    def test_execute_batch_commands_all_uuids_in_selected_device_uuid_list_called(
            aggregator):
        with patch("gsy_e_sdk.aggregator.Aggregator"
                   "._all_uuids_in_selected_device_uuid_list") as mocked_method:
            aggregator.start_websocket_connection()
            aggregator.execute_batch_commands()
            mocked_method.assert_called()

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods",
                             "mock_start_websocket_funcs",
                             "mock_prefixes")
    def test_execute_batch_commands_post_request_called_with_args(aggregator):
        func_input = (f"{'/aggregator_prefix/'}batch-commands",
                      {"aggregator_uuid": TEST_AGGREGATOR_UUID,
                       "batch_commands": TEST_BATCH_COMMAND_DICT}
                      )

        aggregator.aggregator_uuid = TEST_AGGREGATOR_UUID
        aggregator.start_websocket_connection()
        aggregator.execute_batch_commands()
        aggregator._post_request.assert_called_with(*func_input)

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods")
    def test_execute_batch_commands_post_request_not_posted(aggregator):
        aggregator._post_request.return_value = [TEST_TRANSACTION_ID, False]
        assert aggregator.execute_batch_commands() is None

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods", "mock_start_websocket_funcs")
    def test_execute_batch_commands_clear_command_buffer_called(aggregator):
        aggregator.start_websocket_connection()
        aggregator.execute_batch_commands()
        aggregator._client_command_buffer.clear.assert_called_once()

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods", "mock_start_websocket_funcs")
    def test_execute_batch_commands_wait_for_command_response_called(aggregator):
        aggregator.start_websocket_connection()
        aggregator.execute_batch_commands()
        aggregator.dispatcher.wait_for_command_response.assert_called_once()

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods", "mock_start_websocket_funcs")
    def test_execute_batch_commands_returns_response(aggregator):
        aggregator.start_websocket_connection()
        aggregator.dispatcher.wait_for_command_response.return_value = \
            TEST_BATCH_COMMAND_RESPONSE
        response = aggregator.execute_batch_commands()
        assert response == TEST_BATCH_COMMAND_RESPONSE

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods", "mock_start_websocket_funcs")
    def test_execute_batch_commands_log_bid_offer_confirmation_called(aggregator):
        with patch("gsy_e_sdk.aggregator.log_bid_offer_confirmation") as mocked_method:
            aggregator.start_websocket_connection()
            aggregator.dispatcher.wait_for_command_response.return_value = \
                TEST_BATCH_COMMAND_RESPONSE
            aggregator.execute_batch_commands()
            mocked_method.assert_called()

    @staticmethod
    @pytest.mark.usefixtures("mock_execute_batch_command_methods", "mock_start_websocket_funcs")
    def test_execute_batch_commands_log_deleted_bid_offer_confirmation_called(
            aggregator):
        with patch("gsy_e_sdk.aggregator.log_deleted_bid_offer_confirmation"
                   ) as mocked_method:
            aggregator.start_websocket_connection()
            aggregator.dispatcher.wait_for_command_response.return_value = \
                TEST_BATCH_COMMAND_RESPONSE
            aggregator.execute_batch_commands()
            mocked_method.assert_called()


@pytest.mark.parametrize("uuid_mapping, expected_ret_val",
                         [(TEST_AREA_MAPPING, TEST_AREA_MAPPING[TEST_AREA_NAME][0]), ({}, None)])
def test_get_uuid_from_area_name_returns_none_with_empty_dict_(uuid_mapping, expected_ret_val,
                                                               aggregator):
    aggregator.area_name_uuid_mapping = uuid_mapping
    patch("gsy_e_sdk.aggregator.get_uuid_from_area_name_in_tree_dict",
          return_value=TEST_AREA_MAPPING[TEST_AREA_NAME][0])
    assert aggregator.get_uuid_from_area_name(TEST_AREA_NAME) is expected_ret_val


@pytest.mark.parametrize("blocking_get_request_ret_val, expected_ret_val",
                         [([TEST_AGGREGATOR_NAME], [TEST_AGGREGATOR_NAME]), (None, [])])
def test_list_aggregators_request_not_empty_list(blocking_get_request_ret_val, expected_ret_val,
                                                 aggregator):
    with patch("gsy_e_sdk.aggregator.blocking_get_request",
               return_value=blocking_get_request_ret_val):
        aggs_list = aggregator.list_aggregators()
        assert aggs_list == expected_ret_val


@pytest.mark.usefixtures("mock_prefixes")
def test_get_configuration_registry_calls_blocking_get_request(aggregator):
    conf = f"{TEST_CONFIGURATION_PREFIX}registry"

    with patch("gsy_e_sdk.aggregator.blocking_get_request") as mocked_func:
        aggregator.get_configuration_registry()
        mocked_func.assert_called_with(conf, {}, TEST_JWT_KEY_FROM_SERVER)


@pytest.mark.usefixtures("mock_prefixes")
def test_delete_aggregator_calls_blocking_post_request(aggregator):
    conf = f"{aggregator.aggregator_prefix}delete-aggregator/"

    with patch("gsy_e_sdk.aggregator.blocking_post_request") as mocked_func:
        aggregator.delete_aggregator()
        mocked_func.assert_called_with(conf,
                                       {"aggregator_uuid": aggregator.aggregator_uuid},
                                       aggregator.jwt_token)


def test_add_to_batch_commands_returns_client_command_buffer(aggregator):
    buffer_instance = aggregator.add_to_batch_commands
    assert buffer_instance == aggregator._client_command_buffer


@pytest.mark.usefixtures("mock_execute_batch_command_methods")
def test_commands_buffer_length_returns_expected_buffer_length(aggregator):
    assert aggregator.commands_buffer_length == 3
