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
import json
import logging
import os
from datetime import datetime
from logging import getLogger

import click
from click.types import Choice
from click_default_group import DefaultGroup
from colorlog.colorlog import ColoredFormatter

from d3a_api_client.utils import get_simulation_id_from_config_uuid, get_auth_headers_for_requests
from historical_data_api_client.get_historical_data import get_historical_device_profiles
from historical_data_api_client.utils import validate_correct_datetime_format, DATE_TIME_FORMAT

log = getLogger(__name__)


@click.group(name='d3a-api-client', cls=DefaultGroup, default='run', default_if_no_args=True,
             context_settings={'max_content_width': 120})
@click.option('-l', '--log-level', type=Choice(list(logging._nameToLevel.keys())), default='INFO',
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


@main.command()
@click.option('-u', '--username', type=str, help="D3A username", required=None)
@click.option('-p', '--password', type=str, help="D3A password", required=None)
@click.option('-c', '--configuration-id', type=str, help="configuration uuid", required=True)
@click.option('-a', '--area-uuid', type=str, help="area uuid", required=True)
@click.option('-d', '--domain-name', default="https://d3aweb.gridsingularity.com",
              type=str, help="D3A domain URL")
@click.option('-s', '--start-time', type=str, default=None,
              help="start time (Format(YYYY-MM-DDThh:mm)")
@click.option('-e', '--end-time', type=str, default=None,
              help="end time (Format(YYYY-MM-DDThh:mm)")
@click.option('-t', '--time-resolution-hours', type=int, default=1,
              help="time resolution in hours")
@click.option('-r', '--export-file-path', type=str, default=None,
              help="path to export json file")
def run(username, password, domain_name, configuration_id, area_uuid, start_time, end_time,
        time_resolution_hours, export_file_path):
    try:
        if username is None:
            if "API_CLIENT_USERNAME" not in os.environ:
                raise Exception("Please provide a username either via cli or env variable")
        else:
            os.environ["API_CLIENT_USERNAME"] = username
        if password is None:
            if "API_CLIENT_PASSWORD" not in os.environ:
                raise Exception("Please provide a password either via cli or env variable")
        else:
            os.environ["API_CLIENT_PASSWORD"] = password

        if start_time is None:
            start_time = "2018-01-01T00:00"
        if end_time is None:
            end_time = datetime.now().strftime(DATE_TIME_FORMAT)
        validate_correct_datetime_format(start_time)
        validate_correct_datetime_format(end_time)

        auth_headers = get_auth_headers_for_requests(domain_name)
        simulation_id = get_simulation_id_from_config_uuid(domain_name, configuration_id,
                                                           auth_headers)
        profile_data = get_historical_device_profiles(domain_name, auth_headers,
                                                      simulation_id, area_uuid,
                                                      start_time, end_time,
                                                      time_resolution_hours)

        if export_file_path:
            logging.info("Exporting data to %s", export_file_path)
            with open(export_file_path, 'w') as json_file:
                json.dump(profile_data, json_file)
            logging.info("Done")
        else:
            logging.info(f"Requested data: \n {profile_data}")

    except Exception as ex:
        logging.error("Something went wrong: %s", ex)


