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
import logging
import os
import platform
import multiprocessing
import click

from click.types import Choice
from click_default_group import DefaultGroup
from colorlog.colorlog import ColoredFormatter
from logging import getLogger
from multiprocessing import Process

from d3a_interface.exceptions import D3AException


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
@click.option('--setup', 'setup_module_name', default="default_2a",
              help="Simulation setup module use]")
def run(setup_module_name, **kwargs):

    try:
        exec(open(f"./{setup_module_name}").read())

    except D3AException as ex:
        raise click.BadOptionUsage(ex.args[0])
