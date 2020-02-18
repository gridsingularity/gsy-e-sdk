from os import system
import platform
docker_command = "sudo docker" if platform.system() == 'Linux' else "docker"


def before_all(context):
    system('bash integration_tests/build_test_containers.sh')
    system(f'{docker_command} network create integtestnet')


def after_all(context):
    system(f'{docker_command} network rm integtestnet')


def before_scenario(context, scenario):
    pass


def after_scenario(context, scenario):
    # pass
    system(f'{docker_command} stop $({docker_command} ps -a -q) && {docker_command} rm $({docker_command} ps -a -q)')
