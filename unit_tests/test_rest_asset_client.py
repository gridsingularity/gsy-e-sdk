# pylint: disable=missing-function-docstring, no-member, too-many-public-methods
import uuid
from unittest.mock import patch, MagicMock
# import os

import pytest
from gsy_e_sdk.constants import MAX_WORKER_THREADS

from gsy_e_sdk.clients.rest_asset_client import RestAssetClient

TEST_ASSET_UUID = str(uuid.uuid4())
TEST_SIMULATION_ID = str(uuid.uuid4())
TEST_AGGREGATOR_UUID = str(uuid.uuid4())
TEST_TRANSACTION_ID = str(uuid.uuid4())

TEST_DOMAIN_NAME = "test.domain@name.com"
TEST_WEBSOCKETS_DOMAIN_NAME = "wss:test.domain@name.com"

TEST_COMMAND_RESPONSE = {"command": "register",
                         "transaction_id": TEST_TRANSACTION_ID,
                         "registered": True
                         }


@pytest.fixture(name="mock_connections", autouse=True)
def fixture_mock_connections(mocker):
    # gsy_framework/client_connections/utils.py
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.retrieve_jwt_key_from_server")
    mocker.patch("gsy_framework.client_connections.utils.RepeatingTimer")

    mocker.patch("gsy_e_sdk.clients.rest_asset_client.WebsocketThread")
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.ThreadPoolExecutor")


@pytest.fixture(name="mock_environment_use_functions")
def fixture_mock_env_use_functions(mocker):
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.simulation_id_from_env",
                 return_value=TEST_SIMULATION_ID)
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.domain_name_from_env",
                 return_value=TEST_DOMAIN_NAME)
    mocker.patch("gsy_e_sdk.clients.rest_asset_client.websocket_domain_name_from_env",
                 return_value=TEST_WEBSOCKETS_DOMAIN_NAME)


@pytest.fixture(name="mock_transaction_id")
def mock_transaction_id_and_timeout_blocking(mocker):
    mocker.patch("gsy_framework.client_connections.utils.uuid.uuid4",
                 return_value=TEST_TRANSACTION_ID)


@pytest.fixture(name="client")
def fixture_rest_asset_client(mock_environment_use_functions):  # pylint: disable=unused-argument
    return RestAssetClient(asset_uuid=TEST_ASSET_UUID)


@pytest.mark.usefixtures("mock_environment_use_functions")
class TestRestAssetClient:
    """Test methods for RestAssetClient class."""

    @staticmethod
    @pytest.mark.parametrize("set_value, expected_ret_val",
                             [(TEST_SIMULATION_ID, TEST_SIMULATION_ID),
                              (None, TEST_SIMULATION_ID)])
    def test_constructor_simulation_id_setup(set_value, expected_ret_val):
        client = RestAssetClient(asset_uuid=TEST_ASSET_UUID,
                                 simulation_id=set_value)
        assert client.simulation_id == expected_ret_val

    @staticmethod
    @pytest.mark.parametrize("set_value, expected_ret_val",
                             [(TEST_DOMAIN_NAME, TEST_DOMAIN_NAME),
                              (None, TEST_DOMAIN_NAME)])
    def test_constructor_domain_name_setup(set_value, expected_ret_val):
        client = RestAssetClient(asset_uuid=TEST_ASSET_UUID,
                                 domain_name=set_value)
        assert client.domain_name == expected_ret_val

    @staticmethod
    @pytest.mark.parametrize("set_value, expected_ret_val",
                             [(TEST_WEBSOCKETS_DOMAIN_NAME, TEST_WEBSOCKETS_DOMAIN_NAME),
                              (None, TEST_WEBSOCKETS_DOMAIN_NAME)])
    def test_constructor_websockets_domain_name_setup(set_value, expected_ret_val):
        client = RestAssetClient(asset_uuid=TEST_ASSET_UUID,
                                 websockets_domain_name=set_value)
        assert client.websockets_domain_name == expected_ret_val

    @staticmethod
    @pytest.mark.parametrize("set_value, expected_call_val",
                             [(None, TEST_DOMAIN_NAME),
                              ("test_sim_api_name", "test_sim_api_name")])
    def test_constructor_jwt_token_setup(set_value, expected_call_val):
        with patch("gsy_e_sdk.clients.rest_asset_client."
                   "retrieve_jwt_key_from_server") as mocked_func:
            RestAssetClient(asset_uuid=TEST_ASSET_UUID,
                            sim_api_domain_name=set_value)
            mocked_func.assert_called_with(expected_call_val)

    @staticmethod
    def test_start_websocket_connection_device_websocket_message_receiver_instantiated():
        device_websocket_msg_rec_mock = MagicMock()
        with patch("gsy_e_sdk.clients.rest_asset_client.DeviceWebsocketMessageReceiver",
                   return_value=device_websocket_msg_rec_mock):
            client = RestAssetClient(asset_uuid=TEST_ASSET_UUID)
            assert client.dispatcher is device_websocket_msg_rec_mock

    @staticmethod
    def test_start_websocket_connection_websocket_thread_instantiated_and_started():
        websocket_thread_mock = MagicMock()
        with patch("gsy_e_sdk.clients.rest_asset_client.WebsocketThread",
                   return_value=websocket_thread_mock) as mocked_class:
            client = RestAssetClient(asset_uuid=TEST_ASSET_UUID)
            websocket_uri = f"{client.websockets_domain_name}/" \
                            f"{client.simulation_id}/{client.asset_uuid}/"

            mocked_class.assert_called_with(websocket_uri,
                                            client.domain_name,
                                            client.dispatcher)
            assert client.websocket_thread is websocket_thread_mock
            client.websocket_thread.start.assert_called()

    @staticmethod
    def test_start_websocket_connection_thread_pool_executor_instantiated():
        thread_pool_executor_mock = MagicMock()
        with patch("gsy_e_sdk.clients.rest_asset_client.ThreadPoolExecutor",
                   return_value=thread_pool_executor_mock) as mocked_class:
            client = RestAssetClient(asset_uuid=TEST_ASSET_UUID)

            mocked_class.assert_called_with(max_workers=MAX_WORKER_THREADS)
            assert client.callback_thread is thread_pool_executor_mock

    @staticmethod
    def test_endpoint_prefix(client):
        expected_ret_val = f"{client.domain_name}/external-connection/api/" \
                           f"{client.simulation_id}/{client.asset_uuid}"
        assert client.endpoint_prefix == expected_ret_val

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id")
    def test_register_request_post_call(client):
        endpoint = f"{client.endpoint_prefix}/register/"
        data = {"transaction_id": TEST_TRANSACTION_ID}

        with patch("gsy_framework.client_connections.utils.post_request",
                   return_value=None) as mocked_func:
            client.register()
            mocked_func.assert_called_with(endpoint, data, client.jwt_token)

    @staticmethod
    def test_register_request_not_posted_return_none(client):
        with patch("gsy_framework.client_connections.utils.post_request",
                   return_value=False):
            ret_val = client.register()

            assert ret_val is None

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id")
    def test_register_request_posted_waits_for_command_response_and_return_expected(client):
        with patch("gsy_framework.client_connections.utils.post_request",
                   return_value=True):
            client.dispatcher = MagicMock()
            client.dispatcher.wait_for_command_response.return_value = TEST_COMMAND_RESPONSE

            assert client.register() == TEST_COMMAND_RESPONSE
            client.dispatcher.wait_for_command_response.assert_called_with(
                "register", TEST_TRANSACTION_ID, timeout=15 * 60)

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id")
    def test_unregister_request_post_call(client):
        endpoint = f"{client.endpoint_prefix}/unregister/"
        data = {"transaction_id": TEST_TRANSACTION_ID}
        with patch("gsy_framework.client_connections.utils.post_request",
                   return_value=None) as mocked_func:
            client.unregister(is_blocking=False)

            mocked_func.assert_called_with(endpoint, data, client.jwt_token)

    @staticmethod
    def test_unregister_request_not_posted_return_none(client):
        with patch("gsy_framework.client_connections.utils.post_request",
                   return_value=False):
            ret_val = client.unregister(is_blocking=False)

            assert ret_val is None

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id")
    def test_unregister_request_posted_waits_for_command_response_and_return_expected(client):
        with patch("gsy_framework.client_connections.utils.post_request",
                   return_value=True):
            client.dispatcher = MagicMock()
            client.dispatcher.wait_for_command_response.return_value = TEST_COMMAND_RESPONSE

            assert client.unregister(is_blocking=False) == TEST_COMMAND_RESPONSE
            client.dispatcher.wait_for_command_response.assert_called_with(
                "unregister", TEST_TRANSACTION_ID, timeout=15 * 60)

    @staticmethod
    @pytest.mark.parametrize("post_response",
                             [{"aggregator_uuid": TEST_AGGREGATOR_UUID},
                              None])
    def test_select_aggregator_post_request_and_set_active_aggregator(client, post_response):
        endpoint = f"{client.aggregator_prefix}select-aggregator/"
        data = {"aggregator_uuid": TEST_AGGREGATOR_UUID, "device_uuid": client.asset_uuid}
        jwt_token = client.jwt_token

        with patch("gsy_e_sdk.clients.rest_asset_client.blocking_post_request",
                   return_value=post_response) as mocked_func:
            client.select_aggregator(TEST_AGGREGATOR_UUID)
            expected_ret_val = post_response["aggregator_uuid"] if post_response else None

            mocked_func.assert_called_with(endpoint, data, jwt_token)
            assert client.active_aggregator is expected_ret_val

    @staticmethod
    def test_unselect_aggregator_post_request_and_remove_active_aggregator(client):
        endpoint = f"{client.aggregator_prefix}unselect-aggregator/"
        data = {"aggregator_uuid": TEST_AGGREGATOR_UUID, "device_uuid": client.asset_uuid}
        jwt_token = client.jwt_token

        with patch("gsy_e_sdk.clients.rest_asset_client."
                   "blocking_post_request") as mocked_func:
            client.unselect_aggregator(TEST_AGGREGATOR_UUID)

            mocked_func.assert_called_with(endpoint, data, jwt_token)
            assert client.active_aggregator is None

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id")
    def test_set_energy_forecast_post_request(client):
        endpoint = f"{client.endpoint_prefix}/set-energy-forecast/"
        data = {"energy_forecast": {},
                "transaction_id": TEST_TRANSACTION_ID}
        jwt_token = client.jwt_token

        with patch("gsy_framework.client_connections.utils.post_request",
                   return_value=True) as mocked_func:
            client.set_energy_forecast(energy_forecast_kWh={}, do_not_wait=True)

            mocked_func.assert_called_with(endpoint, data, jwt_token)

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id")
    def test_set_energy_forecast_call_wait_for_command_response_and_return_response(client):
        client.dispatcher = MagicMock()
        client.dispatcher.wait_for_command_response.return_value = TEST_COMMAND_RESPONSE
        with patch("gsy_framework.client_connections.utils.post_request", return_value=True):
            ret_val = client.set_energy_forecast(energy_forecast_kWh={})

            client.dispatcher.wait_for_command_response.assert_called_with("set_energy_forecast",
                                                                           TEST_TRANSACTION_ID)
            assert ret_val is TEST_COMMAND_RESPONSE

    @staticmethod
    @pytest.mark.parametrize("posted, do_not_wait", [(True, True), (False, True), (False, False)])
    def test_set_energy_forecast_return_none(client, posted, do_not_wait):
        with patch("gsy_framework.client_connections.utils.post_request",
                   return_value=posted):
            ret_val = client.set_energy_forecast(energy_forecast_kWh={},
                                                 do_not_wait=do_not_wait)
            assert ret_val is None

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id")
    def test_set_energy_measurement_post_request(client):
        endpoint = f"{client.endpoint_prefix}/set-energy-measurement/"
        data = {"energy_measurement": {},
                "transaction_id": TEST_TRANSACTION_ID}
        jwt_token = client.jwt_token

        with patch("gsy_framework.client_connections.utils.post_request",
                   return_value=True) as mocked_func:
            client.set_energy_measurement(energy_measurement_kWh={}, do_not_wait=True)

            mocked_func.assert_called_with(endpoint, data, jwt_token)

    @staticmethod
    @pytest.mark.usefixtures("mock_transaction_id")
    def test_set_energy_measurement_call_wait_for_command_response_and_return_response(client):
        client.dispatcher = MagicMock()
        client.dispatcher.wait_for_command_response.return_value = TEST_COMMAND_RESPONSE
        with patch("gsy_framework.client_connections.utils.post_request", return_value=True):
            ret_val = client.set_energy_measurement(energy_measurement_kWh={})

            client.dispatcher.wait_for_command_response.assert_called_with(
                "set_energy_measurement", TEST_TRANSACTION_ID)
            assert ret_val is TEST_COMMAND_RESPONSE

    @staticmethod
    @pytest.mark.parametrize("posted, do_not_wait", [(True, True), (False, True), (False, False)])
    def test_set_energy_measurement_return_none(client, posted, do_not_wait):
        with patch("gsy_framework.client_connections.utils.post_request",
                   return_value=posted):
            ret_val = client.set_energy_measurement(energy_measurement_kWh={},
                                                    do_not_wait=do_not_wait)
            assert ret_val is None
