from os import system


def before_all(context):
    system(f'docker stop $(docker ps -a -q) && docker rm $(docker ps -a -q)')
    system('bash integration_tests/build_test_containers.sh')
    system(f'docker network create integtestnet')


def after_all(context):
    system(f'docker network rm integtestnet')


def before_scenario(context, scenario):
    pass


def after_scenario(context, scenario):
    system(f'docker stop $(docker ps -a -q) && docker rm $(docker ps -a -q)')
