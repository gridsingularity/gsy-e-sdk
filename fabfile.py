# pylint: disable=not-context-manager

import os

from fabric.colors import green, yellow
from fabric.context_managers import hide
from fabric.decorators import hosts, task
from fabric.operations import local
from fabric.utils import abort, puts


def _ensure_venv():
    if "VIRTUAL_ENV" not in os.environ:
        abort("No active virtualenv found. Please create / activate one before continuing.")


def _pre_check():
    _ensure_venv()
    _ensure_pip_tools()


def _ensure_pip_tools():
    try:
        # pylint: disable-next=unused-import, import-outside-toplevel
        import piptools  # noqa
    except ImportError:
        with hide("running", "stdout"):
            puts(yellow("Installing 'pip-tools'"), show_prefix=True)
            local("pip install pip-tools==4.4.0")


@task
@hosts("localhost")
def compile():  # pylint: disable=redefined-builtin
    """Compile the requirements files (useful to bump dependencies)."""
    _pre_check()
    with hide("running", "stdout"):
        puts(green("Generating package requirement file."), show_prefix=True)
        local("pip-compile --output-file=requirements/base.txt requirements/base.in")
        local("pip-compile --output-file=requirements/dev.txt requirements/dev.in")


@task(default=True)
@hosts("localhost")
def sync():
    """Ensure that the installed packages match requirements."""
    _pre_check()
    with hide("running", "stdout"):
        puts(green("Syncing requirements to local packages."), show_prefix=True)
        local("pip-sync requirements/base.txt")
        local("pip install -e .")
