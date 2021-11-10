"""Setup module for the gsy-e-sdk."""

import os

from setuptools import find_packages, setup
from gsy_e_sdk import __version__

BRANCH = os.environ.get("BRANCH", "master")


try:
    with open("requirements/base.txt", encoding="utf-8") as req:
        REQUIREMENTS = [r.partition("#")[0] for r in req if not r.startswith("-e")]
        REQUIREMENTS.extend(
            ["gsy-framework @ "
             f"git+https://github.com/gridsingularity/gsy-framework@{BRANCH}"])

except OSError:
    # Shouldn't happen
    REQUIREMENTS = []

with open("README.md", "r", encoding="utf-8") as readme:
    README = readme.read()

# *IMPORTANT*: Don't manually change the version here. Use the 'bumpversion' utility.
VERSION = __version__

setup(
    name="gsy-e-sdk",
    description="GSy Exchange Software Development Kit",
    long_description=README,
    author="GridSingularity",
    author_email="d3a@gridsingularity.com",
    url="https://github.com/faizan2590/gsy-e-sdk",
    version=VERSION,
    packages=find_packages(where=".", exclude=["tests"]),
    package_dir={"gsy_e_sdk": "gsy_e_sdk"},
    package_data={},
    install_requires=REQUIREMENTS,
    entry_points={
        "console_scripts": [
            "gsy-e-sdk = gsy_e_sdk.cli:main",
        ]
    },
    zip_safe=False,
)
