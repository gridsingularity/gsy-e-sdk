# GSy Exchange SDK

## Table of Content
- [GSy Exchange SDK](#gsy-e-sdk)
  * [Overview](#overview)
  * [Installation Instructions](#installation-instructions)
  * [How to use the Client](#how-to-use-the-client)
    + [Interacting via CLI](#interacting-via-cli)
    + [Authentication for REST API](#authentication-for-rest-api)
      - [Via CLI](#via-cli)
      - [Via environmental variables](#via-environmental-variables-)
    + [Events](#events)
    + [Asset API](#asset-api)
      - [How to create a connection to an Asset](#how-to-create-a-connection-to-an-asset)
    + [Grid Operator API](#grid-operator-api)
      - [How to create a connection to a Market](#how-to-create-a-connection-to-a-market)
    + [Aggregator Connection](#aggregator-connection)
      - [How to create an Aggregator](#how-to-create-an-aggregator-)
      - [How to select and unselect an Aggregator](#how-to-select-and-unselect-an-aggregator)
      - [How to send batch commands](#how-to-send-batch-commands)
      - [Available batch commands](#available-batch-commands)
    + [How to calculate grid fees](#how-to-calculate-grid-fees)
    + [Hardware API](#hardware-api)
      - [Sending Energy Forecast](#sending-energy-forecast)



## Overview

GSy Exchange SDK is responsible for communicating with a running collaboration of GSy Exchange. The client uses
the API of the GSy Exchange external connections in order to be able to dynamically connect to the simulated
electrical grid and place offers for its energy production, and bids for its energy consumption/requirements.

For local test runs of GSy Exchange Redis (https://redis.io/) is used as communication protocol.
In the following commands for the local test run are marked with `LOCAL`.

For communication with collaborations or canary networks on https://d3a.io, a RESTful API is used.
In the following commands for the connection via the REST API are marked with `REST`.

## Installation Instructions

Installation of gsy-e-sdk using pip:

```
pip install git+https://github.com/gridsingularity/gsy-e-sdk.git
```
---

## How to use the Client

### Interacting via CLI
In order to get help, please run:
```
gsy-e-sdk run --help
```
The following parameters can be set via the CLI:
- `base-setup-path` --> Path where user's client script resides, otherwise `gsy_e_sdk/setups` is used.
- `setup` --> Name of user's API client module/script.
- `username` --> Username of agent authorized to communicate with respective collaboration or Canary Network (CN).
- `password` --> Password of respective agent
- `domain-name` --> GSy Exchange domain URL
- `web-socket` --> GSy Exchange websocket URL
- `simulation-id` --> UUID of the collaboration or Canary Network (CN)
- `simulation-config-path` --> Path to the JSON file that contains the user's collaboration or CN information.
  This file can be downloaded from the "Registry" page on the GSy Exchange website.
  `simulation-id`, `domain-name`, and `web-socket` CLI-settings will be overwritten by the settings in the file
- `run-on-redis` --> This flag can be set for local testing of the API client, where no user authentication is required.
  For that, a locally running redis server and GSy Exchange simulation are needed.

#### Examples
- For local testing of the API client:
  ```
  gsy-e-sdk --log-level ERROR run --setup test_redis_aggregator --run-on-redis
  ```
- For testing your api client script on remote server hosting GSy Exchange's collaboration/CNs.
    - If user's client script resides on `gsy_e_sdk/setups`
    ```
    gsy-e-sdk run -u <username> -p <password> --setup test_create_aggregator --simulation-config-path <your-downloaded-simulation-config-file-path>
    ```
    - If user's client script resides on a different directory, then its path needs to be set via `--base-setup-path`
    ```
    gsy-e-sdk run -u <username> -p <password> --base-setup-path <absolute/relative-path-to-your-client-script> --setup <name-of-your-script> --simulation-config-path <your-downloaded-simulation-config-file-path>
    ```

---


### Events
In order to facilitate offer and bid management and scheduling,
the client will get notified via events.
It is possible to capture these events and perform operations as a reaction to them
by overriding the corresponding methods.
- when a new market cycle is triggered the `on_market_cycle` method is called
- when a new tick has started, the `on_tick` method is called
- when the simulation has finished, the `on_finished` method is called
- when any event arrives , the `on_event_or_response` method is called
---

### Asset API
#### How to create a connection to an Asset
The constructor of the API class can connect and register automatically to a running collaboration:
- `REST`
  (here the asset uuid has to be obtained first)
    ```
    asset_uuid = get_area_uuid_from_area_name_and_collaboration_id(
                  <simulation_id>, <asset_name>, <domain_name>
                  )
    asset_client = RestAssetClient(asset_uuid, autoregister=True)
    ```
- `LOCAL`
    ```
    asset_client = RedisClient(<slugified-asset-name>, autoregister=True)
    ```

Otherwise one can connect manually:
```
asset_client.register()
```
To disconnect/unregistering, the following command is available:
```
asset_client.unregister()
```

---

### Grid Operator API
#### How to create a connection to a Market
- `REST`
    (here the market uuid has to be obtained first)
    ```
    market_uuid = get_area_uuid_from_area_name_and_collaboration_id(
                  <simulation_id>, <market_name>, <domain_name>
                  )
    market_client = RestMarketClient(market_uuid, autoregister=True)
    ```
- `LOCAL`
    ```
    market_client = RedisMarketClient(<market_name>, autoregister=True)
    ```

### Aggregator Connection

Aggregators are clients that control multiple assets and/or markets and can send out batch
commands in order to react to an event simultaneously for each owned asset.

#### How to create an Aggregator

- `REST`
    ```
    aggregator = Aggregator(
            simulation_id=<simulation_id>,
            domain_name=<domain_name>,
            aggregator_name=<aggregator_name>,
            websockets_domain_name=<websocket_domain_name>
            )
    ```
- `LOCAL`
    ```
    aggregator = AutoAggregator(<aggregator_name>)
    ```

#### How to list your aggregators

To list your aggregators, its configuration id and the registered assets, you should:
```python
```python
from gsy_e_sdk.utils import get_aggregators_list
my_aggregators = get_aggregators_list(domain_name="Domain Name")
```
The returned value is a list of aggregators and their connected assets
```python
[{'configUuid': 'f7330248-9a72-4979-8477-dfbcff0c46a0', 'name': 'My aggregator',
 'devicesList': [{"deviceUuid":"My_asset_uuid"},{"deviceUuid":"My_asset_uuid 2"}]}]
```

#### How to select and unselect an Aggregator

The asset or market can select the Aggregator
(assuming that a [connection to an asset was established](#how-to-create-a-connection-to-an-asset)):
```
asset.select_aggregator(aggregator.aggregator_uuid)
```
The asset or market can unselect the Aggregator:
```
asset.unselect_aggregator(aggregator.aggregator_uuid)
```

#### Available Aggregator's methods

`Aggregator` instances provide methods that can simplify specific operations. Below we list some of the most commonly used:

- Return all the aggregators connected to the aggregator's simulation:
    ```python
    list_aggregators()
    ```

- Return the representation of all the assets and areas connected to the aggregator's configuration:
    ```python
    get_configuration_registry()
    ```

- Delete the current aggregator:
    ```python
    delete_aggregator()
    ```

#### How to send batch commands

Commands to all or individual connected assets or markets can be sent in one batch.
All asset or market specific functions can be sent via commands that are
accumulated and added to buffer.

```
aggregator.add_to_batch_commands.bid_energy(<asset_uuid>, <energy>, <price_cents>)
```
These also can be chained as follow:
```
aggregator.add_to_batch_commands.bid_energy(<asset_uuid>, <energy>, <price_cents>)\
                                .offer_energy(<asset_uuid>, <energy>, <price_cents>)\
                                .asset_info(<asset_uuid>)
```
Finally, the batch commands are sent to the GSy Exchange via the following command:
```
aggregator.execute_batch_command()
```

#### Available batch commands

The following commands can be issued as batch commands (refer to [How to send batch commands](#how-to-send-batch-commands) for more information):

- Send an energy bid with price in cents:
    ```python
    bid_energy(area_uuid, energy, price_cents, replace_existing, attributes, requirements)
    ```
- Send an energy bid with energy rate in cents/kWh:
    ```python
    bid_energy_rate(area_uuid, energy, rate_cents_per_kWh, replace_existing, attributes, requirements)
    ```
- Change grid fees using a percentage value:
    ```python
    change_grid_fees_percent(area_uuid, fee_percent)
    ```
- Change grid fees using a constant value:
    ```python
    grid_fees(area_uuid, fee_cents_kwh)
    ```
- Delete offer using its ID:
    ```python
    delete_offer(area_uuid, offer_id)
    ```
- Delete bid using its ID:
    ```python
    delete_bid(area_uuid, bid_id)
    ```
- Get asset info (returns demanded energy for Load assets and available energy for PVs):
    ```python
    asset_info(area_uuid)
    ```
- List all posted offers:
    ```python
    list_offers(area_uuid)
    ```
- Lists all posted bids:
    ```python
    list_bids(area_uuid)
    ```
- Retrieve market DSO statistics:
    ```python
    last_market_dso_stats(area_uuid)
    ```
- Send an energy offer with price in cents:
    ```python
    offer_energy(area_uuid, energy, price_cents, replace_existing, attributes, requirements)
    ```
- Send an energy offer with energy rate in cents/kWh:
    ```python
    offer_energy_rate(area_uuid, energy, rate_cents_per_kWh, replace_existing, attributes, requirements)
    ```
---
### Attributes and requirements

A finer control over the issued bids and offers can be achieved through the `attributes` and `requirements` parameters of the bid and offer methods.

#### `attributes`

A dictionary of attributes that describe the offer or bid. Currently supported attributes:

- Offers:
    - `energy_type`: energy type of the offer

- Bids: Not supported at the moment

#### `requirements`

A list of dictionaries containing requirements for the offer or bid. At least one of the provided dictionaries needs to be satisfied in the matching process. Currently supported requirements:

- Offers:
    - `trading_partners`: IDs of the areas with which the offer has to be matched

- Bids:
    - `energy_type`: energy types that the bid prefers to consume
    - `trading_partners`: IDs of the areas with which the bid prefers to be matched
    - `energy`: energy that the bid prefers to consume
    - `price`: trade rate that the bid prefers to accept

### How to calculate grid fees
The `Aggregator` class has a function that calculates the grid fees along path between two assets or
markets in the grid:
```
Aggregator.calculate_grid_fee(start_market_or_asset_name, target_market_or_asset_name, fee_type):
```
The algorithm retrieves the path between `start_market_or_asset_name` and `target_market_or_asset_name`
and accumulates all corresponding grid fees along the way. Market and asset names are supported.
`target_market_or_asset_name` is optional, if left blank, only the grid fee of the
`start_market_or_asset_name` is returned.
The user can chose between `current_market_fee` and `last_market_fee`, which is toggled by providing
the corresponding string in the `fee_type` input parameter.

---

### Hardware API

#### Sending Energy Forecast
##### With gsy-e-sdk
The energy consumption or demand for PV and Load assets can be set for the next market slot via
the following command
(assuming that a [connection to an asset was established](#how-to-create-a-connection-to-an-asset)):
```
asset_client.set_energy_forecast(<energy_forecast_Wh>)
```
An example how this command could be added into an aggregator script can be found in
**gsy_e_sdk/setups/test_sending_energy_forecast.py** .

##### Directly via REST endpoint
In case the user wants to send asset measurements without using the API client, the raw REST API
can be used instead. An additional authentication step has to be performed first.

###### Authentication with JWT
Authentication is done via JSON web token (JWT). In order to retrieve the JWT, the following POST
request has to be performed:
```
POST https://d3aweb-dev.gridsingularity.com/api-token-auth/
```
The body of the request needs to contain the following information (JSON string):
```
{"username": "<your_username>", "password": "<your_password>"}
```
The returned JWT needs to be sent via the Authorization HTTP header when sending the forecast data.
For that you need to add the following key value pair to the header of every POST command:
```
Authorization: JWT <your_token>
```
###### Send energy forecast
The POST to send the energy value is the following
(please fill in `<Canary Network UUID>` and `<Asset UUID>`):
```
POST https://d3aweb-dev.gridsingularity.com/external-connection/api/<Canary Network UUID>/<Asset UUID>/set_energy_forecast/
```
The body of the request needs to contain the following information (JSON string):
```
{"energy_Wh": <energy_value_for_asset>}
```
