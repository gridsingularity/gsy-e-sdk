import unittest
import os
import inspect
from jsonschema import ValidationError

from d3a_api_client.cli import create_simulation_config_path
import d3a_api_client
from d3a_api_client.utils import read_simulation_config_file, domain_name_from_env, \
    websocket_domain_name_from_env, simulation_id_from_env, get_sim_id_and_domain_names
from unit_tests import FIXTURES_DIR


class TestSimulationInfo(unittest.TestCase):

    def setUp(self) -> None:
        os.environ.pop('SIMULATION_CONFIG_FILE_PATH', None)
        self.api_client_path = os.path.dirname(inspect.getsourcefile(d3a_api_client))

    def tearDown(self) -> None:
        os.environ.pop('SIMULATION_CONFIG_FILE_PATH', None)

    def test_get_sim_id_and_domain_names_returns_the_defaults(self):
        simulation_id, domain_name, websockets_domain_name = get_sim_id_and_domain_names()
        assert simulation_id == simulation_id_from_env()
        assert domain_name == domain_name_from_env()
        assert websockets_domain_name == websocket_domain_name_from_env()

    def test_simulation_info_file_is_correctly_parsed_json_file(self):
        config_file_path = os.path.join(
            FIXTURES_DIR, 'api-client-summary-84d221fa-46e1-49e0-823b-26cfb3425a5a.json'
        )
        config = read_simulation_config_file(config_file_path)
        assert config["uuid"] == "84d221fa-46e1-49e0-823b-26cfb3425a5a"
        assert config["domain_name"] == "localhost"
        assert config["web_socket_domain_name"] == "ws://localhost/external-ws"

    def test_assertion_is_raised_if_incorrect_info_is_provided(self):
        with self.assertRaises(ValidationError):
            read_simulation_config_file(os.path.join(
                FIXTURES_DIR,
                'malformed-api-client-summary-84d221fa-46e1-49e0-823b-26cfb3425a5a.json'
            ))

    def test_default_simulation_info_is_under_setup_module_if_base_path_isnt_provided(self):
        input_config_file_path = 'test.json'
        config_file_path = create_simulation_config_path(
            base_setup_path=None, simulation_config_path=input_config_file_path
        )
        assert config_file_path == input_config_file_path

    def test_default_simulation_info_file_is_under_base_path(self):
        base_path = "/Users/test.user/somefolder"
        input_config_file_path = 'test.json'
        config_file_path = create_simulation_config_path(
            base_setup_path=base_path, simulation_config_path=input_config_file_path
        )
        assert config_file_path == os.path.join(base_path, 'test.json')
