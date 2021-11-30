import unittest
import os
import inspect
from jsonschema import ValidationError

import gsy_e_sdk
from gsy_e_sdk.utils import read_simulation_config_file, get_sim_id_and_domain_names
from unit_tests import FIXTURES_DIR
from gsy_e_sdk.constants import DEFAULT_DOMAIN_NAME, DEFAULT_WEBSOCKET_DOMAIN, \
    API_CLIENT_SIMULATION_ID


class TestSimulationInfo(unittest.TestCase):

    def setUp(self) -> None:
        self.api_client_path = os.path.dirname(inspect.getsourcefile(gsy_e_sdk))

    def tearDown(self) -> None:
        os.environ.pop("API_CLIENT_SIMULATION_ID", None)
        os.environ.pop("API_CLIENT_DOMAIN_NAME", None)
        os.environ.pop("API_CLIENT_WEBSOCKET_DOMAIN_NAME", None)

    def test_get_sim_id_and_domain_names_returns_correct_env_values(self):
        os.environ["API_CLIENT_SIMULATION_ID"] = "test-simulation-id"
        os.environ["API_CLIENT_DOMAIN_NAME"] = "test-domain-name"
        os.environ["API_CLIENT_WEBSOCKET_DOMAIN_NAME"] = "test-websocket-name"

        simulation_id, domain_name, websockets_domain_name = get_sim_id_and_domain_names()
        assert simulation_id == "test-simulation-id"
        assert domain_name == "test-domain-name"
        assert websockets_domain_name == "test-websocket-name"

    def test_get_sim_id_and_domain_names_returns_the_defaults(self):
        simulation_id, domain_name, websockets_domain_name = get_sim_id_and_domain_names()
        assert simulation_id == API_CLIENT_SIMULATION_ID
        assert domain_name == DEFAULT_DOMAIN_NAME
        assert websockets_domain_name == DEFAULT_WEBSOCKET_DOMAIN

    def test_simulation_info_file_is_correctly_parsed_json_file(self):
        config_file_path = os.path.join(
            FIXTURES_DIR, "api-client-summary-84d221fa-46e1-49e0-823b-26cfb3425a5a.json"
        )
        config = read_simulation_config_file(config_file_path)
        assert config["uuid"] == "84d221fa-46e1-49e0-823b-26cfb3425a5a"
        assert config["domain_name"] == "localhost"
        assert config["web_socket_domain_name"] == "ws://localhost/external-ws"

    def test_assertion_is_raised_if_incorrect_info_is_provided(self):
        with self.assertRaises(ValidationError):
            read_simulation_config_file(os.path.join(
                FIXTURES_DIR,
                "malformed-api-client-summary-84d221fa-46e1-49e0-823b-26cfb3425a5a.json"
            ))
