import unittest
import os
import inspect
from jsonschema import ValidationError

from d3a_api_client.cli import create_simulation_config_path
import d3a_api_client
from d3a_api_client.utils import get_simulation_config, DOMAIN_NAME_FROM_ENV, \
    WEBSOCKET_DOMAIN_NAME_FROM_ENV, SIMULATION_ID_FROM_ENV
from d3a_interface.api_simulation_config import ApiSimulationConfigException
from unit_tests import FIXTURES_DIR


class TestSimulationInfo(unittest.TestCase):

    def setUp(self) -> None:
        os.environ.pop('SIMULATION_CONFIG_FILE_PATH', None)
        self.api_client_path = os.path.dirname(inspect.getsourcefile(d3a_api_client))

    def tearDown(self) -> None:
        os.environ.pop('SIMULATION_CONFIG_FILE_PATH', None)

    def test_simulation_info_returns_the_default_when_no_json_file_path(self):
        simulation_id, domain_name, websockets_domain_name = get_simulation_config()
        assert simulation_id == SIMULATION_ID_FROM_ENV
        assert domain_name == DOMAIN_NAME_FROM_ENV
        assert websockets_domain_name == WEBSOCKET_DOMAIN_NAME_FROM_ENV

    def test_simulation_info_file_is_correctly_parsed_json_file(self):
        os.environ['SIMULATION_CONFIG_FILE_PATH'] = os.path.join(
            FIXTURES_DIR, 'api-client-summary-84d221fa-46e1-49e0-823b-26cfb3425a5a.json'
        )
        simulation_id, domain_name, websockets_domain_name = get_simulation_config()
        assert simulation_id == "84d221fa-46e1-49e0-823b-26cfb3425a5a"
        assert domain_name == "localhost"
        assert websockets_domain_name == "ws://localhost/external-ws"

    def test_assertion_is_raised_if_incorrect_info_is_provided(self):
        with self.assertRaises(ValidationError):
            os.environ['SIMULATION_CONFIG_FILE_PATH'] = os.path.join(
                FIXTURES_DIR,
                'malformed-api-client-summary-84d221fa-46e1-49e0-823b-26cfb3425a5a.json'
            )
            get_simulation_config()

    def test_default_simulation_info_is_under_setup_module_if_base_path_isnt_provided(self):
        os.environ["SIMULATION_CONFIG_FILE_PATH"] = create_simulation_config_path(
            base_setup_path=None, simulation_config_path='test.json'
        )
        assert os.environ['SIMULATION_CONFIG_FILE_PATH'] == os.path.join(self.api_client_path,
                                                                         'setups/test.json')

    def test_default_simulation_info_file_is_under_base_path(self):
        base_path = "/Users/test.user/somefolder"
        os.environ["SIMULATION_CONFIG_FILE_PATH"] = create_simulation_config_path(
            base_setup_path=base_path, simulation_config_path='test.json'
        )
        assert os.environ['SIMULATION_CONFIG_FILE_PATH'] == os.path.join(base_path, 'test.json')

    def test_exception_is_raised_if_simulation_info_filename_not_provided(self):
        base_path = "/Users/test.user/somefolder"
        with self.assertRaises(ApiSimulationConfigException):
            os.environ["SIMULATION_CONFIG_FILE_PATH"] = create_simulation_config_path(
                base_setup_path=base_path, simulation_config_path=None
            )
