from behave import given, when, then
from os import system
from time import sleep
from integration_tests.test_load_connection import AutoBidOnLoadDevice
from integration_tests.test_pv_connection import AutoOfferOnPVDevice
from integration_tests.environment import docker_command


@given('redis container is started')
def step_impl(context):
    system(f'{docker_command} run -d -p 6379:6379 --name redis.container -h redis.container '
           '--net integtestnet gsyd3a/d3a:redis-staging')


@given('d3a container is started using setup file {setup_file}')
def step_impl(context, setup_file):
    sleep(3)
    system(f'{docker_command} run -d --env REDIS_URL=redis://redis.container:6379/ --net integtestnet --name d3a-api-test  d3a-tests '
           f'run -t 2s -s 60m --setup {setup_file}')


@when('the external client is started with test_load_connection')
def step_impl(context):
    # Wait for d3a to activate all areas
    sleep(5)
    # Connects one client to the load device
    context.load = AutoBidOnLoadDevice('load', autoregister=True, redis_url='redis://localhost:6379/')


@when('the external client is started with test_pv_connection')
def step_impl(context):
    # Wait for d3a to activate all areas
    sleep(5)
    # Connects one client to the load device
    context.load = AutoOfferOnPVDevice('pv', autoregister=True, redis_url='redis://localhost:6379/')


@then('the external client is connecting to the simulation until finished')
def step_impl(context):
    # Infinite loop in order to leave the client running on the background
    # placing bids and offers on every market cycle.
    # Should stop if an error occurs or if the simulation has finished
    counter = 0  # Wait for five minutes at most
    while context.load.errors == 0 and context.load.status != "finished" and counter < 300:
        print(counter, context.load.status)
        sleep(3)
        counter += 3


@then('the external client does not report errors')
def step_impl(context):
    print(context.load.errors)
    assert context.load.errors == 0


@then('the energy bills of the load report the required energy was bought by the load')
def step_impl(context):
    print(context.load.market_info)
    assert context.load.market_info["device_bill"]["bought"] == (24 * 4 - 2) * 0.05
