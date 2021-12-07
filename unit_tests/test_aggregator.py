# pylint: disable=missing-function-docstring
from unittest.mock import patch, PropertyMock
import pytest

from gsy_e_sdk.aggregator import Aggregator, AggregatorWebsocketMessageReceiver
from unit_tests.constants import (test_simulation,
                                  test_agg, test_batch_command_dict,
                                  test_response, TEST_AGGREGATOR_PREFIX,
                                  TEST_CONFIGURATION_PREFIX)


#####################################
#            FIXTURES               #

@pytest.fixture(autouse=True)
def mock_connections(mocker):
    """Fixture mocks methods and functions which establish external connections"""
    mocker.patch("gsy_e_sdk.aggregator.blocking_get_request",
                 return_value=test_agg, autospec=True)
    mocker.patch("gsy_e_sdk.aggregator.blocking_post_request",
                 return_value=test_agg, autospec=True)
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.retrieve_jwt_key_from_server",
                 return_value="some_key")
    mocker.patch("gsy_framework.client_connections.utils.RestCommunicationMixin"
                 + "._create_jwt_refresh_timer")
    mocker.patch("gsy_e_sdk.aggregator.Aggregator._connect_to_simulation",
                 autospec=True)


@pytest.fixture(autouse=True)
def mock_prefixes(mocker):
    """
    Fixture mocks external prefix calculation functions
    """
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.get_aggregator_prefix",
                 return_value=TEST_AGGREGATOR_PREFIX, autospec=True)
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.get_configuration_prefix",
                 return_value=TEST_CONFIGURATION_PREFIX)


@pytest.fixture(autouse=True)
def mock_start_websocket_funcs(mocker):
    """
    Fixture mocks external classes used in Aggregator.start_websocket_connections
    """
    # mocker.patch("gsy_e_sdk.aggregator.AggregatorWebsocketMessageReceiver")
    mocker.patch("gsy_e_sdk.aggregator.WebsocketThread", autospec=True)
    mocker.patch("gsy_e_sdk.aggregator.ThreadPoolExecutor",
                 autospec=True,
                 return_value="test_set_callback_thread")


@pytest.fixture(autouse=True)
def mock_execute_batch_command_methods(mocker):
    """
    Fixture mocks methods and external functions and other methods used in
    execute_batch_command.
    """
    mocker.patch("gsy_e_sdk.aggregator.ClientCommandBuffer.buffer_length",
                 new_callable=PropertyMock, return_value=3)
    mocker.patch("gsy_e_sdk.aggregator.ClientCommandBuffer.execute_batch",
                 return_value=test_batch_command_dict)
    mocker.patch("gsy_e_sdk.aggregator.Aggregator._post_request",
                 return_value=["some_transaction_id", True])
    mocker.patch("gsy_e_sdk.aggregator.AggregatorWebsocketMessageReceiver"
                 + ".wait_for_command_response",
                 return_value=test_response)
    mocker.patch("gsy_e_sdk.aggregator.Aggregator."
                 + "_all_uuids_in_selected_device_uuid_list",
                 return_value=True)
    mocker.patch("gsy_e_sdk.aggregator.log_bid_offer_confirmation")
    mocker.patch("gsy_e_sdk.aggregator.log_deleted_bid_offer_confirmation")


@pytest.fixture(name="aggregator_explicit")
def fixture_aggregator_explicit():
    """
    Fixture creates and returns a aggregator with explicit parameters.
    """
    return Aggregator(aggregator_name=test_agg["name"],
                      simulation_id=test_simulation["uuid"],
                      domain_name=test_simulation["domain_name"],
                      websockets_domain_name=test_simulation["websockets_domain_name"]
                      )


@pytest.fixture(name="aggregator_from_env")
def fixture_aggregator_from_env(mocker):
    """
    Fixture creates and returns an aggregator with implicit parameters.
    """
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.simulation_id_from_env",
                 return_value=test_simulation["uuid"])
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.domain_name_from_env",
                 return_value=test_simulation["domain_name"])
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.websocket_domain_name_from_env",
                 return_value=test_simulation["websockets_domain_name"])

    return Aggregator(aggregator_name=test_agg["name"])


#####################################
# DEF TESTS FOR CLASSES CONSTRUCTOR #

class TestConstructor:
    """Includes methods which test constructor."""

    @staticmethod
    def test_constructor__simulation_id_explicit(aggregator_explicit):
        # this attribute is inherited from rest_asset_client
        agg = aggregator_explicit
        assert agg.simulation_id == test_simulation["uuid"]

    @staticmethod
    def test_constructor__simulation_id_from_env(aggregator_from_env):
        # this attribute is inherited from rest_asset_client
        assert aggregator_from_env.simulation_id == test_simulation["uuid"]

    @staticmethod
    def test_constructor__domain_name_explicit(aggregator_explicit):
        # this attribute is inherited from rest_asset_client
        assert aggregator_explicit.domain_name == test_simulation["domain_name"]

    @staticmethod
    def test_constructor__domain_name_from_env(aggregator_from_env):
        # this attribute is inherited from rest_asset_client
        assert aggregator_from_env.domain_name == test_simulation["domain_name"]

    @staticmethod
    def test_constructor__websockets_domain_name_explicit(aggregator_explicit):
        # this attribute is inherited from rest_asset_client
        assert aggregator_explicit.websockets_domain_name == \
               test_simulation["websockets_domain_name"]

    @staticmethod
    def test_constructor__websockets_domain_name_from_env(aggregator_from_env):
        # this attribute is inherited from rest_asset_client
        assert aggregator_from_env.websockets_domain_name == \
               test_simulation["websockets_domain_name"]

    @staticmethod
    def test_constructor__asset_uuid(aggregator_explicit):
        # this attribute is inherited from rest_asset_client
        assert aggregator_explicit.asset_uuid == ""

    @staticmethod
    def test_constructor__jwt_token(aggregator_explicit):
        # this attribute is inherited from rest_asset_client
        assert aggregator_explicit.jwt_token == "some_key"

    @staticmethod
    def test_constructor__aggregator_prefix(aggregator_explicit):
        # this attribute is inherited from rest_asset_client
        assert aggregator_explicit.aggregator_prefix == "/aggregator_prefix/"

    @staticmethod
    def test_constructor__configuration_prefix(aggregator_explicit):
        # this attribute is inherited from rest_asset_client
        assert aggregator_explicit.configuration_prefix == "/configuration_prefix/"

    @staticmethod
    def test_constructor__grid_fee_calculation():
        with patch("gsy_e_sdk.aggregator.GridFeeCalculation", autospec=True) as mocked_class:
            Aggregator(aggregator_name=test_agg["name"])
            mocked_class.assert_called_once()

    # *
    @staticmethod
    def test_constructor__aggregator_name(aggregator_explicit):
        assert aggregator_explicit.aggregator_name == test_agg["name"]

    # *
    @staticmethod
    @pytest.mark.parametrize("accept", [True, False])
    def test_constructor__accept_all_devices(accept):
        agg = Aggregator(aggregator_name=test_agg["name"],
                         accept_all_devices=accept)
        assert agg.accept_all_devices == accept

    # *
    @staticmethod
    def test_constructor__device_uuid_list(aggregator_explicit):
        assert aggregator_explicit.device_uuid_list == []

    # *
    @staticmethod
    def test_constructor__aggregator_uuid(aggregator_explicit):
        assert aggregator_explicit.aggregator_uuid is None

    @staticmethod
    def test_constructor__client_command_buffer():
        with patch("gsy_e_sdk.aggregator.ClientCommandBuffer") as mocked_class:
            Aggregator(aggregator_name=test_agg["name"])
            mocked_class.assert_called_once()

    @staticmethod
    def test_constructor__connect_to_simulation():
        with patch.object(Aggregator, "_connect_to_simulation") as mocked_method:
            Aggregator(aggregator_name=test_agg["name"])
            mocked_method.assert_called_once()

    # *
    @staticmethod
    def test_constructor__latest_grid_tree(aggregator_explicit):
        assert aggregator_explicit.latest_grid_tree == {}

    # *
    @staticmethod
    def test_constructor__latest_grid_tree_flat(aggregator_explicit):
        assert aggregator_explicit.latest_grid_tree_flat == {}

    # *
    @staticmethod
    def test_constructor__area_name_uuid_mapping(aggregator_explicit):
        assert aggregator_explicit.area_name_uuid_mapping == {}


class TestStartWebsocketConnection:
    """Includes methods which test start_websocket_connection method."""

    @staticmethod
    def test_start_websocket_connection__dispatcher(aggregator_explicit):
        with patch("gsy_e_sdk.aggregator.AggregatorWebsocketMessageReceiver") \
                as mocked_class:
            aggregator_explicit.start_websocket_connection()
            mocked_class.assert_called()

    @staticmethod
    def test_start_websocket_connection__websocket_creation(aggregator_explicit):
        websocket_uri_test = f"{test_simulation['websockets_domain_name']}" \
                             + f"/{test_simulation['uuid']}/aggregator/" \
                             + f"{test_agg['uuid']}/"

        with patch("gsy_e_sdk.aggregator.WebsocketThread") as mocked_class:
            aggregator_explicit.aggregator_uuid = test_agg["uuid"]
            aggregator_explicit.start_websocket_connection()
            mocked_class.assert_called_with(websocket_uri_test, test_simulation["domain_name"],
                                            aggregator_explicit.dispatcher)

    @staticmethod
    def test_start_websocket_connection__websocket_start(aggregator_explicit):
        aggregator_explicit.start_websocket_connection()
        aggregator_explicit.websocket_thread.start.assert_called_once()

    @staticmethod
    def test_start_websocket_connection__callback_thread(aggregator_explicit):
        aggregator_explicit.start_websocket_connection()
        assert aggregator_explicit.callback_thread == "test_set_callback_thread"


class TestGetUuidFromAreaName:
    """Includes methods which test get_uuid_from_area_name method."""

    @staticmethod
    def test_get_uuid_from_area_name__not_empty_dict(aggregator_explicit):
        aggregator_explicit.area_name_uuid_mapping = {"TestArea": ["test_area_uuid"]}
        area_name = "TestArea"

        patch("gsy_e_sdk.aggregator.get_uuid_from_area_name_in_tree_dict",
              return_value="test_area_uuid")
        assert aggregator_explicit.get_uuid_from_area_name(area_name) == "test_area_uuid"

    @staticmethod
    def test_get_uuid_from_area_name__empty_dict(aggregator_explicit):
        aggregator_explicit.area_name_uuid_mapping = {}
        area_name = "TestArea"

        patch("gsy_e_sdk.aggregator.get_uuid_from_area_name_in_tree_dict",
              return_value="test_area_uuid")
        assert aggregator_explicit.get_uuid_from_area_name(area_name) is None


class TestCalculateGridFee:
    """Includes methods which test calculate_grid_fee method."""

    @staticmethod
    def test_calculate_grid_fee__called(aggregator_explicit):
        with patch("gsy_e_sdk.aggregator.GridFeeCalculation.calculate_grid_fee") as mocked_method:
            aggregator_explicit.calculate_grid_fee("start_market",
                                                   "target_market",
                                                   "current_market_fee")
            mocked_method.assert_called_with("start_market",
                                             "target_market",
                                             "current_market_fee")

    @staticmethod
    def test_calculate_grid_fee__return(aggregator_explicit):
        with patch("gsy_e_sdk.aggregator.GridFeeCalculation.calculate_grid_fee",
                   return_value="test_ok"):
            ret_val = aggregator_explicit.calculate_grid_fee("start_market",
                                                             "target_market",
                                                             "current_market_fee")
            assert ret_val == "test_ok"


class TestExecuteBatchCommands:
    """Includes methods which test execute_batch_commands method."""

    @staticmethod
    def test_execute_batch_commands__command_buffer_length_zero(aggregator_explicit):
        with patch("gsy_e_sdk.aggregator.ClientCommandBuffer.buffer_length",
                   new_callable=PropertyMock, return_value=0):
            assert aggregator_explicit.execute_batch_commands() is None

    @staticmethod
    def test_execute_batch_commands__execute_batch_called(aggregator_explicit):
        with patch("gsy_e_sdk.aggregator.ClientCommandBuffer.execute_batch") as mocked_method:
            aggregator_explicit.start_websocket_connection()
            aggregator_explicit.execute_batch_commands()
            mocked_method.assert_called_once()

    @staticmethod
    def test_execute_batch_commands__validate_all_uuids_in_list(aggregator_explicit):
        with patch("gsy_e_sdk.aggregator.Aggregator"
                   + "._all_uuids_in_selected_device_uuid_list") as mocked_method:
            aggregator_explicit.start_websocket_connection()
            aggregator_explicit.execute_batch_commands()
            mocked_method.assert_called()

    @staticmethod
    def test_execute_batch_commands__post_request_called_with_args(aggregator_explicit):
        args_ = (f"{'/aggregator_prefix/'}batch-commands",
                 {"aggregator_uuid": test_agg["uuid"], "batch_commands": test_batch_command_dict}
                 )

        with patch("gsy_e_sdk.aggregator.Aggregator._post_request",
                   return_value=["test_transaction_id", True]) as mocked_method:
            aggregator_explicit.aggregator_uuid = test_agg["uuid"]
            aggregator_explicit.start_websocket_connection()
            aggregator_explicit.execute_batch_commands()
            mocked_method.assert_called_with(*args_)

    @staticmethod
    def test_execute_batch_commands__post_request_not_posted(aggregator_explicit):
        with patch("gsy_e_sdk.aggregator.Aggregator._post_request",
                   return_value=["test_transaction_id", False]):
            assert aggregator_explicit.execute_batch_commands() is None

    @staticmethod
    def test_execute_batch_commands__clear_command_buffer(aggregator_explicit):
        with patch("gsy_e_sdk.aggregator.ClientCommandBuffer.clear") as mocked_method:
            aggregator_explicit.start_websocket_connection()
            aggregator_explicit.execute_batch_commands()
            mocked_method.assert_called_once()

    @staticmethod
    def test_execute_batch_commands__wait_for_command_response_called(aggregator_explicit):
        with patch.object(AggregatorWebsocketMessageReceiver, "wait_for_command_response",
                          return_value=test_response) as mocked_method:
            aggregator_explicit.start_websocket_connection()
            aggregator_explicit.execute_batch_commands()
            mocked_method.assert_called_once()

    @staticmethod
    def test_execute_batch_commands__return_response(aggregator_explicit):
        with patch.object(AggregatorWebsocketMessageReceiver, "wait_for_command_response",
                          return_value=test_response):
            aggregator_explicit.start_websocket_connection()
            response = aggregator_explicit.execute_batch_commands()
            assert response == test_response

    @staticmethod
    def test_execute_batch_commands__log_bid__offer_confirmation_called(aggregator_explicit):
        with patch("gsy_e_sdk.aggregator.log_bid_offer_confirmation") as mocked_method:
            aggregator_explicit.start_websocket_connection()
            aggregator_explicit.execute_batch_commands()
            mocked_method.assert_called()

    @staticmethod
    def test_execute_batch_commands__log_deleted_bid_offer_confirmation_called(
            aggregator_explicit):
        with patch("gsy_e_sdk.aggregator.log_deleted_bid_offer_confirmation") \
                as mocked_method:
            aggregator_explicit.start_websocket_connection()
            aggregator_explicit.execute_batch_commands()
            mocked_method.assert_called()


@pytest.mark.parametrize("aggregator_specs", [test_agg, None])
def test_list_aggregators(aggregator_specs, aggregator_explicit):
    with patch("gsy_e_sdk.aggregator.blocking_get_request", return_value=aggregator_specs):
        aggs_list = aggregator_explicit.list_aggregators()

        if aggregator_specs is not None:
            assert aggs_list == test_agg
        else:
            assert aggs_list == []


def test_get_configuration_registry(aggregator_explicit):
    conf = f"{TEST_CONFIGURATION_PREFIX}registry"

    with patch("gsy_e_sdk.aggregator.blocking_get_request") as mocked_func:
        aggregator_explicit.get_configuration_registry()
        mocked_func.assert_called_with(conf, {}, "some_key")


def test_delete_aggregator(aggregator_explicit):
    conf = f"{TEST_AGGREGATOR_PREFIX}delete-aggregator/"

    with patch("gsy_e_sdk.aggregator.blocking_post_request") as mocked_func:
        aggregator_explicit.delete_aggregator()
        mocked_func.assert_called_with(conf, {"aggregator_uuid": None}, "some_key")


def test_add_to_batch_commands(aggregator_explicit):
    # pylint: disable=protected-access
    buffer_instance = aggregator_explicit.add_to_batch_commands
    assert buffer_instance == aggregator_explicit._client_command_buffer


def test_commands_buffer_length(aggregator_explicit):
    patch("gsy_e_sdk.aggregator.ClientCommandBuffer.buffer_length",
          new_callable=PropertyMock, return_value=3)
    assert aggregator_explicit.commands_buffer_length == 3
