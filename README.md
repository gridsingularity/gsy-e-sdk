# D3A API Client

## Table of Content
- [D3A API Client](#d3a-api-client)
  * [Overview](#overview)
  * [Installation Instructions](#installation-instructions)
  * [How to use the Client](#how-to-use-the-client)
    + [Events](#events)
    + [Trading API](#trading-api)
    + [Market/DSO API](#marketdso-api)
    + [Aggregator Connection](#aggregator-connection)
    + [Hardware API](#hardware-api)


## Overview

D3A API client is responsible for communicating with a running collaboration of D3A. The client uses 
the API of the D3A external connections in order to be able to dynamically connect to the simulated 
electrical grid and place offers for its energy production, and bids for its energy consumption/requirements.

For local test runs of D3A Redis (https://redis.io/) is used as communication protocol. 
In the following commands for the local test run are marked with `LOCAL`. 

For communication with collaborations or canary networks on https://d3a.io, a RESTful API is used.
In the following commands for the connection via the REST API are marked with `REST`. 

## Installation Instructions

Installation of d3a-api-client using pip:

```
pip install git+https://github.com/gridsingularity/d3a-api-client.git
```

---
## How to use the Client
In the following an overview of the functionality of the client and the connection to the D3A is given.
Code examples can be found under the `tests/` folder.

### Events
In order to facilitate offer and bid management and scheduling, 
the client will get notified via events. 
It is possible to capture these events and perform operations as a reaction to them
by overriding the corresponding methods.
- when a new market cycle is triggered the `on_market_cycle` method is called
- when a new tick has started, the `on_tick` method is called
- when the simulation has finished, the `on_finished` method is called
---

### Trading API
#### How to create a connection to a Device
The constructor of the API class can connect and register automatically to a running collaboration:
- `REST`
  (here the device uuid has to be obtained first)
    ```
    device_uuid = get_area_uuid_from_area_name_and_collaboration_id(
                  <simulation_id>, <device_name>, <domain_name>
                  )
    device_client = RestDeviceClient(device_uuid, autoregister=True)
    ```
- `LOCAL`
    ``` 
    device_client = RedisClient(<slugified-device-name>, autoregister=True)
    ```

Otherwise one can connect manually:
```
device_client.register()
```
To disconnect/unregistering, the following command is available:
```
device_client.unregister()
```

#### Available device commands:
- Send an energy offer with price in cents:
    ```device_client.offer_energy(<energy>, <price_cents>)```
- Send an energy offer with energy rate in cents/kWh:
    ```device_client.offer_energy_rate(<energy, <rate_cents_per_kWh>)```
- Send an energy bid with price in cents: 
    ```device_client.bid_energy(<energy>, <price_cents>)```
- Send an energy bid with energy rate in cents/kWh:
    ```device_client.bid_energy_rate(<energy, <rate_cents_per_kWh>)```
- List all posted offers:
    ```device_client.list_offers()```
- Lists all posted bids
    ```device_client.list_bids()```
- Delete offer using its id
    ```device_client.delete_offer(<offer_id)```
- Delete bid using its id
    ```device_client.delete_bid(<bid_id)```
- Get device info (returns demanded energy for Load devices and available energy for PVs)
    ```device_client.device_info()```
---

### Market/DSO API
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
#### Available market commands:
- list statistics: 
    ```
    market_client.last_market_dso_stats()
    market_client.last_market_stats()
    ```
- change grid fees: 

    `market_client.grid_fees(<constant_grid_fees_cent_per_kWh>)`
---

### Aggregator Connection
Aggregators are clients that control multiple devices and/or markets and can send out batch 
commands in order to react to an event simultaneously for each owned device.
#### How to create an Aggregator:
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
#### How to select and unselect an Aggregator
The device or market can select the Aggregator 
(assuming that a [connection to a device was established](#how-to-create-a-connection-to-a-device)):
```
device.select_aggregator(aggregator.aggregator_uuid)
```
The device or market can unselect the Aggregator:
```
device.unselect_aggregator(aggregator.aggregator_uuid)
```

#### How to send batch commands
Commands all or individual connected devices or markets can be send in one batch.
All device or market specific functions can be sent via commands that are
dictionary representations of the functions and their parameters.
An individual command is a dictionary that contains the keys `type` and `data`. With `type` one selects 
the function to be called for the device or market and `data` contains the parameters needed for the
function.
For example, the call of 
```
device_client.bid_energy(<energy>, <price_cents>)
```
translates to 
```
{"type": "bid_energy", "data": {"energy": <energy>, "price": <price_cents>}}
```

These dictionary commands should be accumulated in a dictionary with `area_uuids` (device or market uuids) 
as keys and the list of batch commands as values:
```
batch_commands={
    <area1_uuid>: [{"type": "list_bids", "data": {}}, 
                   {"type": "bid_energy", "data": {"energy": 1, "price": 2}}],
    <area2_uuid>: [{"type": "list_bids", "data": {}}]
                }
```
Finally, the batch commands are sent to the D3A via the following command:
```
aggregator.batch_command(batch_commands)
```
---

### Hardware API

#### Sending Energy Forecast 
The energy consumption or demand for PV and Load devices can be set for the next market slot via
the following command 
(assuming that a [connection to a device was established](#how-to-create-a-connection-to-a-device)):
```
device_client.set_power_forecast(<power_power_forecast_W>)
```

---