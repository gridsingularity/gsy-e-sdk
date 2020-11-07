import logging
import traceback
from os import environ, getpid
from datetime import datetime
from redis import StrictRedis
from rq import Connection, Worker, get_current_job, Queue
from rq.decorators import job
from mqtt_subscriber.oli_broker import MQTTConnection
from d3a_api_client.utils import get_area_uuid_from_area_name_and_collaboration_id, \
    list_running_canary_networks_and_devices_with_live_data
from d3a_api_client.rest_device import RestDeviceClient


allowed_device_names = ["DOSE/OLI_42"]


@job('canary_mqtt')
def start(configuration_id, live_data_device_names):
    logging.getLogger().setLevel(logging.DEBUG)

    job = get_current_job()
    job.save_meta()

    device_args = {
        "simulation_id": configuration_id,
        "domain_name": environ["API_CLIENT_DOMAIN_NAME"],
        "websockets_domain_name": environ["API_CLIENT_WEBSOCKET_DOMAIN_NAME"],
        "autoregister": True
    }

    mqtt_rest_api_clients_mapping = {}

    # Construct the api client
    for device_name in live_data_device_names:
        if device_name in allowed_device_names:
            device_uuid = get_area_uuid_from_area_name_and_collaboration_id(
                device_args["simulation_id"], device_name, device_args["domain_name"])
            mqtt_rest_api_clients_mapping[device_name] = RestDeviceClient(device_id=device_uuid, **device_args)

    # Connect to the MQTT broker
    mqtt_connection = MQTTConnection(mqtt_rest_api_clients_mapping)
    try:
        mqtt_connection.run_forever()
    except Exception as e:
        logging.error(f"MQTT Subscriber failed with error {e}")
        logging.error(traceback.format_exc())


def connect_to_running_canary_networks():
    cns = list_running_canary_networks_and_devices_with_live_data(environ["API_CLIENT_DOMAIN_NAME"])

    if not cns:
        return

    redis_conn = StrictRedis.from_url(environ.get('REDIS_URL', 'redis://localhost'), retry_on_timeout=True)
    queue = Queue(name="canary_mqtt", connection=redis_conn)
    for config_uuid, live_data_devices in cns.items():
        queue.enqueue("mqtt_redis_job.start", config_uuid, live_data_devices)


def main():
    with Connection(StrictRedis.from_url(environ.get('REDIS_URL', 'redis://localhost'),
                                         retry_on_timeout=True)):
        Worker(
            ['canary_mqtt'],
            name='mqtt_subscriber.{}.{:%s}'.format(getpid(), datetime.now()), log_job_description=False
        ).work()


if __name__ == "__main__":
    connect_to_running_canary_networks()
    main()
