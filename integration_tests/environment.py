from os import system


def before_all(context):
    system(f'docker stop $(docker ps -a -q) 2>&1 && docker rm $(docker ps -a -q) 2>&1')
    system(f'docker network create integtestnet 2>&1')
    system('bash integration_tests/build_test_containers.sh 2>&1')


def after_all(context):
    system(f'docker network rm integtestnet')


def before_scenario(context, scenario):
    pass


def after_scenario(context, scenario):
    context.device.redis_thread.stop()
    context.device.redis_db.close()
    system(f'docker stop $(docker ps -a -q) > /dev/null && '
           f'docker rm $(docker ps -a -q) > /dev/null ')
