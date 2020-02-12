from behave import given, when, then
from os import system
from time import sleep
from integration_tests.test_load_connection import AutoBidOnLoadDevice
from integration_tests.test_pv_connection import AutoOfferOnPVDevice
from integration_tests.test_ess_bid_connection import AutoBidOnESSDevice


@given('redis container is started')
def step_impl(context):
    system('docker run -d -p 6379:6379 --name redis.container -h redis.container '
           '--net integtestnet gsyd3a/d3a:redis-staging')


@given('d3a container is started using setup file {setup_file}')
def step_impl(context, setup_file):
    sleep(3)
    system(f'docker run -d --env REDIS_URL=redis://redis.container:6379/ --net integtestnet -t d3a '
           f'  -l DEBUG run -t 1s -s 60m --setup {setup_file}')


@when('the external client is started with test_load_connection')
def step_impl(context):
    # Wait for d3a to activate all areas
    sleep(5)
    # Connects one client to the load device
    context.device = AutoBidOnLoadDevice('load', autoregister=True, redis_url='redis://localhost:6379/')


@when('the external client is started with test_pv_connection')
def step_impl(context):
    # Wait for d3a to activate all areas
    sleep(5)
    # Connects one client to the load device
    context.device = AutoOfferOnPVDevice('pv', autoregister=True, redis_url='redis://localhost:6379/')

@when('the external client is started with test_ess_bid_connection')
def step_impl(context):
    # Wait for d3a to activate all areas
    sleep(5)
    # Connects one client to the load device
    context.device = AutoBidOnESSDevice('storage', autoregister=True,
                                         redis_url='redis://localhost:6379/')

@then('the external client is connecting to the simulation until finished')
def step_impl(context):
    # Infinite loop in order to leave the client running on the background
    # placing bids and offers on every market cycle.
    # Should stop if an error occurs or if the simulation has finished
    counter = 0  # Wait for five minutes at most
    while context.device.errors == 0 and context.device.status != "finished" and counter < 300:
        print(f"context.load.status: {context.device.status}")
        sleep(0.5)
        counter += 0.5


@then('the external client does not report errors')
def step_impl(context):
    print(f"ERROR_LIST: {context.device.error_list}")
    assert context.device.errors == 0
    assert False


@then('the energy bills of the load report the required energy was bought by the load')
def step_impl(context):
    assert context.device.latest_stats["device_stats"]["bills"]["bought"] == (24 * 4 - 2) * 0.05
