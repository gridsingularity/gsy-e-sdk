"""
Copyright 2018 Grid Singularity
This file is part of Grid Singularity Exchange.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
# pylint: disable=too-many-arguments
import importlib
import inspect
import logging
import os
import sys
from logging import getLogger

import click
from click.types import Choice
from click_default_group import DefaultGroup
from colorlog import ColoredFormatter
from gsy_framework.exceptions import GSyException
from gsy_framework.utils import iterate_over_all_modules

import gsy_e_sdk
import gsy_e_sdk.setups as setups
from gsy_e_sdk.constants import SETUP_FILE_PATH
from gsy_e_sdk.utils import domain_name_from_env, websocket_domain_name_from_env, \
    simulation_id_from_env, read_simulation_config_file

log = getLogger(__name__)
gsy_e_sdk_path = os.path.dirname(inspect.getsourcefile(gsy_e_sdk))


@click.group(name="gsy-e-sdk", cls=DefaultGroup, default="run", default_if_no_args=True,
             context_settings={"max_content_width": 120})
@click.option("-l", "--log-level", type=Choice(list(logging._nameToLevel.keys())), default="ERROR",
              show_default=True, help="Log level")
def main(log_level):
    handler = logging.StreamHandler()
    handler.setLevel(log_level)
    handler.setFormatter(
        ColoredFormatter(
            "%(log_color)s%(asctime)s.%(msecs)03d %(levelname)-8s (%(lineno)4d) %(name)-30s: "
            "%(message)s%(reset)s",
            datefmt="%H:%M:%S"
        )
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(handler)


modules_path = setups.__path__ if SETUP_FILE_PATH is None else [SETUP_FILE_PATH, ]
_setup_modules = iterate_over_all_modules(modules_path)


@main.command()
@click.option("-b", "--base-setup-path", default=None, type=str,
              help="Accept absolute or relative path for client script")
@click.option("--setup", "setup_module_name", required=True,
              help="Setup module of client script. Available modules: ["
                   f"{', '.join(_setup_modules)}]")
@click.option("-u", "--username", default=None, type=str, help="D3A username")
@click.option("-p", "--password", default=None, type=str, help="D3A password")
@click.option("-d", "--domain-name", default=None,
              type=str, help="D3A domain URL")
@click.option("-w", "--web-socket", default=None,
              type=str, help="D3A websocket URL")
@click.option("-i", "--simulation-config-path", type=str, default=None,
              help="Path to simulation config file.")
@click.option("-s", "--simulation-id", type=str, default=None,
              help="Simulation id")
@click.option("--run-on-redis", is_flag=True, default=False,
              help="Start the client using the Redis API")
def run(base_setup_path, setup_module_name, username, password, domain_name, web_socket,
        simulation_config_path, simulation_id, run_on_redis):
    if username is not None:
        os.environ["API_CLIENT_USERNAME"] = username
    if password is not None:
        os.environ["API_CLIENT_PASSWORD"] = password

    os.environ["API_CLIENT_RUN_ON_REDIS"] = "true" if run_on_redis else "false"
    if not run_on_redis:
        if simulation_config_path is not None:
            config = read_simulation_config_file(simulation_config_path)
            os.environ["API_CLIENT_DOMAIN_NAME"] = config["domain_name"]
            os.environ["API_CLIENT_WEBSOCKET_DOMAIN_NAME"] = config["web_socket_domain_name"]
            os.environ["API_CLIENT_SIMULATION_ID"] = config["uuid"]
        else:
            os.environ["API_CLIENT_DOMAIN_NAME"] = (
                domain_name if domain_name else domain_name_from_env())
            os.environ["API_CLIENT_WEBSOCKET_DOMAIN_NAME"] = (
                web_socket if web_socket else websocket_domain_name_from_env())
            os.environ["API_CLIENT_SIMULATION_ID"] = (
                simulation_id if simulation_id else simulation_id_from_env())

        validate_general_settings_are_set()

    load_client_script(base_setup_path, setup_module_name)


def validate_general_settings_are_set():
    """Validate if general settings are set."""
    settings_list = ["API_CLIENT_DOMAIN_NAME", "API_CLIENT_WEBSOCKET_DOMAIN_NAME",
                     "API_CLIENT_SIMULATION_ID"]
    for setting in settings_list:
        if os.environ.get(setting) is None:
            raise ValueError(f"{setting} was not set, please provide")


def load_client_script(base_setup_path, setup_module_name):
    """Load client script."""
    try:
        if base_setup_path is None:
            importlib.import_module(f"gsy_e_sdk.setups.{setup_module_name}")
        else:
            sys.path.append(base_setup_path)
            importlib.import_module(setup_module_name)

    except GSyException as ex:
        raise click.BadOptionUsage(ex.args[0], "Error when loading setup module.")
    except ModuleNotFoundError:
        log.exception("Could not find the specified module")
