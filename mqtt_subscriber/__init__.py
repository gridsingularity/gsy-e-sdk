import json
import logging
from os import environ
from d3a_api_client.utils import get_area_uuid_and_name_mapping_from_simulation_id, \
    list_running_canary_networks_and_devices_with_live_data


allowed_devices_name_mapping = {
    "OLI_42": "DOSE/OLI_42/Meter/activeEnergy/Demand",
    "OLI_6": "DOSE/OLI_6/PV/activeEnergy/Supply",
    "OLI_7": "DOSE/OLI_7/PV/activeEnergy/Supply",
    "OLI_8": "DOSE/OLI_8/Meter/activeEnergy/Demand",
    "OLI_9": "DOSE/OLI_9/Meter/activeEnergy/Demand",
    "OLI_28": "DOSE/OLI_28/PV/activeEnergy/Supply",
    "OLI_77": "DOSE/OLI_77/Meter/activeEnergy/Demand",
    "OLI_62": "WIRCON/OLI_62/PV/activeEnergy/Supply",
    "OLI_61": "WIRCON/OLI_61/PV/activeEnergy/Supply",
    "OLI_26": "WIRCON/OLI_26/PV/activeEnergy/Supply",
    "OLI_24": "WIRCON/OLI_24/PV/activeEnergy/Supply",
    "OLI_23": "WIRCON/OLI_23/PV/activeEnergy/Supply"
}


def generate_topic_api_client_args_mapping():
    cn_mapping = list_running_canary_networks_and_devices_with_live_data(environ["API_CLIENT_DOMAIN_NAME"])

    topic_api_client_mapping = {v: [] for _, v in allowed_devices_name_mapping.items()}

    logging.info(f"CN mapping {cn_mapping}")

    for configuration_id, live_data_device_mapping in cn_mapping.items():

        if not live_data_device_mapping:
            logging.info(f"No live data for CN {configuration_id}.")
            continue

        live_data_device_mapping = json.loads(live_data_device_mapping)

        api_client_args = {
            "simulation_id": configuration_id,
            "domain_name": environ["API_CLIENT_DOMAIN_NAME"],
            "websockets_domain_name": environ["API_CLIENT_WEBSOCKET_DOMAIN_NAME"],
            "autoregister": False,
            "start_websocket": False
        }

        live_data_device_uuids = live_data_device_mapping.keys()

        if not live_data_device_uuids:
            logging.info(f"No live data for CN {configuration_id}.")
            continue

        name_uuid_mapping = \
            get_area_uuid_and_name_mapping_from_simulation_id(configuration_id, environ["API_CLIENT_DOMAIN_NAME"])
        # It is a prerequisite to have unique area names for this to work.
        uuid_name_mapping = {v: k for k, v in name_uuid_mapping.items()}

        for device_uuid in live_data_device_uuids:
            device_name = uuid_name_mapping[device_uuid]
            if device_name in allowed_devices_name_mapping:
                topic_name = allowed_devices_name_mapping[device_name]
                topic_api_client_mapping[topic_name].append({
                    "device_id": device_uuid,
                    **api_client_args
                })

    logging.info(f"Connecting to the following MQTT topics/devices {topic_api_client_mapping}")
    return topic_api_client_mapping
