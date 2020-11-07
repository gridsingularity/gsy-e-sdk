import json
import logging
import traceback
from os import environ, getpid
from datetime import datetime
from redis import StrictRedis
from rq import Connection, Worker, get_current_job, Queue
from rq.decorators import job
from mqtt_subscriber.oli_broker import MQTTConnection
from d3a_api_client.utils import get_area_uuid_and_name_mapping_from_simulation_id, \
    list_running_canary_networks_and_devices_with_live_data
from d3a_api_client.rest_device import RestDeviceClient


allowed_device_names = ["DOSE/OLI_42/Meter"]


@job('canary_mqtt')
def start(configuration_id, live_data_device_mapping):
    logging.info(
        f"MQTT client job started with parameters: \n"
        f"Configuration uuid: {configuration_id}\n"
        f"Live data device mapping: {live_data_device_mapping}")

    logging.getLogger().setLevel(logging.INFO)

    job = get_current_job()
    job.save_meta()

    live_data_device_mapping = json.loads(live_data_device_mapping)

    device_args = {
        "simulation_id": configuration_id,
        "domain_name": environ["API_CLIENT_DOMAIN_NAME"],
        "websockets_domain_name": environ["API_CLIENT_WEBSOCKET_DOMAIN_NAME"],
        "autoregister": True
    }

    mqtt_rest_api_clients_mapping = {}

    live_data_device_uuids = live_data_device_mapping.keys()

    if not live_data_device_uuids:
        logging.info("No live data, exiting MQTT subscriber.")
        return

    name_uuid_mapping = \
        get_area_uuid_and_name_mapping_from_simulation_id(configuration_id, environ["API_CLIENT_DOMAIN_NAME"])

    # It is a prerequisite to have unique area names for this to work.
    uuid_name_mapping = {v: k for k, v in name_uuid_mapping.items()}

    # Construct the api client
    for device_uuid in live_data_device_uuids:
        device_name = uuid_name_mapping[device_uuid]
        device_name = device_name.replace("#", "/")
        if device_name in allowed_device_names:
            mqtt_rest_api_clients_mapping[device_name] = RestDeviceClient(device_id=device_uuid, **device_args)

    logging.info(f"Connecting to the following MQTT devices {mqtt_rest_api_clients_mapping}")

    if not mqtt_rest_api_clients_mapping:
        logging.info("No live device data mapping, exiting MQTT subscriber.")
        return

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
            name='mqtt_subscriber.{}.{:%s}'.format(getpid(), datetime.now()), log_job_description=True
        ).work()


if __name__ == "__main__":
    connect_to_running_canary_networks()
    main()
