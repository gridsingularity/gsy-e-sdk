"""
Copyright 2018 Grid Singularity
This file is part of D3A.

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
import importlib
import logging
import click
import os

from click.types import Choice
from click_default_group import DefaultGroup
from colorlog.colorlog import ColoredFormatter
from logging import getLogger

from d3a_interface.exceptions import D3AException
from d3a_interface.utils import iterate_over_all_modules

import d3a_api_client.setups as setups
from d3a_api_client.constants import SETUP_FILE_PATH, DEFAULT_DOMAIN_NAME, DEFAULT_WEBSOCKET_DOMAIN


log = getLogger(__name__)


@click.group(name='d3a-api-client', cls=DefaultGroup, default='run', default_if_no_args=True,
             context_settings={'max_content_width': 120})
@click.option('-l', '--log-level', type=Choice(list(logging._nameToLevel.keys())), default='ERROR',
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
@click.option('--setup', 'setup_module_name', default="auto_offer_bid_on_device",
              help="Setup module of client script. Available modules: [{}]".format(
                  ', '.join(_setup_modules)))
@click.option('-u', '--username', default=None, type=str, help="D3A username")
@click.option('-p', '--password', default=None, type=str, help="D3A password")
@click.option('-d', '--domain-name', default=DEFAULT_DOMAIN_NAME,
              type=str, help="D3A domain name")
@click.option('-w', '--web-socket', default=DEFAULT_WEBSOCKET_DOMAIN,
              type=str, help="D3A websocket URL")
@click.option('--run-on-redis', is_flag=True, default=False, help="Start the client using the Redis API")
def run(setup_module_name, username, password, domain_name, web_socket, run_on_redis, **kwargs):
    if username is not None:
        os.environ["API_CLIENT_USERNAME"] = username
    if password is not None:
        os.environ["API_CLIENT_PASSWORD"] = password
    os.environ["API_CLIENT_DOMAIN_NAME"] = domain_name
    os.environ["API_CLIENT_WEBSOCKET_DOMAIN_NAME"] = web_socket
    os.environ["API_CLIENT_RUN_ON_REDIS"] = "true" if run_on_redis else "false"

    try:
        importlib.import_module(f"d3a_api_client.setups.{setup_module_name}")

    except D3AException as ex:
        raise click.BadOptionUsage(ex.args[0])
    except ModuleNotFoundError as ex:
        log.error("Could not find the specified module")
