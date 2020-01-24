# D3A API Client

D3A API client is responsible for communicating with a running simulation of D3A. The client uses the API of the D3A external connections in order to be able to dynamically connect to the simulated electrical grid and place offers for its energy production, and bids for its energy consumption/requirements.
In order to use this client, a running D3A simulation is required, where the client will be able to connect. At the moment, Redis is the only communication protocol supported, therefore D3A should be configured to use Redis for messaging. HTTP support is under development.


# Installation Instructions

You can install D3A API Client using pip:

```pip install git+https://github.com/gridsingularity/d3a-api-client.git```


# How to use the client

The constructor of the API class can connect and register automatically to a running simulation:


```
r = RedisClient('house-2', 'my-load-device', autoregister=True)
```

Otherwise one can connect manually:

```
r = RedisClient('house-2', 'my-load-device', autoregister=False)

r.register()
```

To disconnect/unregister from a simulation, the following command is available:


```
r.unregister()
```

The available commands are listed:

```
# Sends an energy offer
r.offer_energy(2, 3)
# Sends an energy bid
r.bid_energy(2, 3)
# Lists all posted offers
r.list_offers()
# Lists all posted bids
r.list_bids()
# Delete offer using its id
r.delete_offer('1d352704-d787-4aa1-b1ad-dc10e22b2fc2')
# Delete bid using its id
r.delete_bid('1d352704-d787-4aa1-b1ad-dc10e22b2fc2')
```

In order to facilitate offer and bid management and scheduling, when a new market slot is available the client will get notified via an event. It is possible to capture this event and perform operations after it by overriding the `on_market_cycle` method.

Examples for using this client can be found under `tests/`
