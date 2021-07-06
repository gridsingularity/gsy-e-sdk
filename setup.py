import os
from setuptools import setup, find_packages
from d3a_api_client import __version__

target_branch = os.environ.get('BRANCH', 'master')


try:
    with open('requirements/base.txt') as req:
        REQUIREMENTS = [r.partition('#')[0] for r in req if not r.startswith('-e')]
        REQUIREMENTS.extend(
            ['d3a-interface @ '
             f'git+https://github.com/gridsingularity/d3a-interface.git@{ target_branch }'])

except OSError:
    # Shouldn't happen
    REQUIREMENTS = []

with open("README.md", "r") as readme:
    README = readme.read()

# *IMPORTANT*: Don't manually change the version here. Use the 'bumpversion' utility.
VERSION = __version__

setup(
    name="d3a-api-client",
    description="D3A API Client",
    long_description=README,
    author='GridSingularity',
    author_email='d3a@gridsingularity.com',
    url='https://github.com/gridsingularity/d3a-api-client',
    version=VERSION,
    packages=find_packages(where=".", exclude=["tests"]),
    package_dir={"d3a_api_client": "d3a_api_client"},
    package_data={},
    install_requires=REQUIREMENTS,
    entry_points={
        'console_scripts': [
            'd3a-api-client = d3a_api_client.cli:main',
            'historical-data-api-client = historical_data_api_client.cli:main',
        ]
    },
    zip_safe=False,
)
