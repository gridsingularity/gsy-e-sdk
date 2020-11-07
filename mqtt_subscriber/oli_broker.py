import os
import logging
import traceback
import paho.mqtt.client as mqtt


MQTT_PORT = 1883
MINUTES_IN_HOUR = 60
MEASUREMENT_PERIOD_MINUTES = 15


class MQTTConnection:
    def __init__(self, api_client_dict):
        self.api_clients = api_client_dict
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc):
        logging.debug(f"Connected with result code {str(rc)}")

        for device_name in self.api_clients:
            client.subscribe(f"{device_name}/activeEnergy/*")

    def on_message(self, client, userdata, msg):
        logging.debug(f"{msg.topic} {str(msg.payload)}")

        try:
            device_name = msg.topic.split("/activeEnergy/")[0]
            energy = msg.payload["value"]
            power = energy * (MINUTES_IN_HOUR / MEASUREMENT_PERIOD_MINUTES)
            self.api_clients[device_name].set_power_forecast(power)
        except Exception as e:
            logging.error(f"API Client failed to send power forecasts to d3a with error {e}. "
                          f"Resuming operation.")
            logging.error(f"{traceback.format_exc()}")

    def run_forever(self):
        logging.debug("Creating MQTT client")
        username = os.environ["MQTT_USERNAME"]
        password = os.environ["MQTT_PASSWORD"]
        domain_name = os.environ["MQTT_DOMAIN_NAME"]

        self.client.username_pw_set(username, password=password)
        self.client.connect(domain_name, MQTT_PORT)

        self.client.loop_forever()
