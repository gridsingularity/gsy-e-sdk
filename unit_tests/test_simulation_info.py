import unittest
import os
import inspect
import d3a_api_client

from d3a_api_client.utils import get_simulation_info, domain_name_from_env, \
    websocket_domain_name_from_env, simulation_id_from_env, SimulationInfoException
from cli import set_simulation_file_env


class TestSimulationInfo(unittest.TestCase):

    def setUp(self) -> None:
        os.environ['JSON_FILE_PATH'] = ""
        self.api_client_path = os.path.dirname(inspect.getsourcefile(d3a_api_client))

    def tearDown(self) -> None:
        os.environ['JSON_FILE_PATH'] = ""

    def test_simulation_info_returns_the_default_when_no_json_file_path(self):
        simulation_id, domain_name, websockets_domain_name = get_simulation_info()
        assert simulation_id == simulation_id_from_env
        assert domain_name == domain_name_from_env
        assert websockets_domain_name == websocket_domain_name_from_env

    def test_simulation_info_file_is_correctly_parsed_json_file(self):
        os.environ['JSON_FILE_PATH'] = os.path.join(
            self.api_client_path,
            "resources/api-client-summary-84d221fa-46e1-49e0-823b-26cfb3425a5a.json"
        )
        simulation_id, domain_name, websockets_domain_name = get_simulation_info()
        assert simulation_id == "84d221fa-46e1-49e0-823b-26cfb3425a5a"
        assert domain_name == "localhost"
        assert websockets_domain_name == "ws://localhost/external-ws"

    def test_assertion_is_raised_if_incorrect_info_is_provided(self):
        with self.assertRaises(SimulationInfoException):
            os.environ['JSON_FILE_PATH'] = os.path.join(
                self.api_client_path,
                "resources/malformed-api-client-summary-84d221fa-46e1-49e0-823b-26cfb3425a5a.json"
            )
            get_simulation_info()

    def test_default_simulation_info_is_under_setup_module_if_base_path_isnt_provided(self):
        set_simulation_file_env(base_setup_path=None, simulation_info='test.json',
                                run_on_redis=False)
        assert os.environ['JSON_FILE_PATH'] == os.path.join(self.api_client_path,
                                                            'setups/test.json')

    def test_default_simulation_info_file_is_under_base_path(self):
        base_path = "/Users/test.user/somefolder"
        set_simulation_file_env(base_setup_path=base_path, simulation_info='test.json',
                                run_on_redis=False)
        assert os.environ['JSON_FILE_PATH'] == os.path.join(base_path, 'test.json')

    def test_exception_is_raised_if_simulation_info_filename_not_provided(self):
        base_path = "/Users/test.user/somefolder"
        with self.assertRaises(SimulationInfoException):
            set_simulation_file_env(base_setup_path=base_path, simulation_info=None,
                                    run_on_redis=False)
