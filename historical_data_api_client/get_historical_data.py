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

import requests


def get_historical_device_profiles(domain_name: str, auth_headers: dict,
                                   simulation_id: str, area_uuid: str,
                                   start_time: str, end_time: str,
                                   time_resolution_hrs: int) -> dict:
    """
    Perform http request to historical device data api.

    Args:
        domain_name: url of the selected d3a backend
        auth_headers: dict containing authentication and response format information
        simulation_id: uuid of simulation run
        area_uuid: uuid of area
        start_time: start time string
        end_time: end time string
        time_resolution_hrs: expected time resolution in hours

    Returns:
        dict that contains the requested profile data
    """

    url_prefix = f"{domain_name}/historical-data/devices/"
    url_params = f"{simulation_id}/{area_uuid}/{start_time}/{end_time}/{time_resolution_hrs}"
    resp = requests.get(f"{url_prefix}{url_params}", headers=auth_headers)
    if resp.status_code != 200:
        raise Exception(f"Error when trying to get the profile ({resp.status_code}): {resp.text}")

    return json.loads(resp.text)
