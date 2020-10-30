import os
import paho.mqtt.client as mqtt


MQTT_PORT = 1883


class MQTTConnection:
    def __init__(self, api_client_dict):
        self.api_clients = api_client_dict
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code "+str(rc))

        for device_name in self.api_clients:
            client.subscribe(f"{device_name}/activeEnergy/*")

    def on_message(self, client, userdata, msg):
        print(msg.topic+" "+str(msg.payload))

        try:
            device_name = msg.topic.split("/activeEnergy/")[0]
            self.api_clients[device_name].set_power_forecast(msg.payload["value"])
        except Exception as e:
            print(e)

    def run_forever(self):
        print("Creating MQTT client")
        username = os.environ["MQTT_USERNAME"]
        password = os.environ["MQTT_PASSWORD"]
        domain_name = os.environ["MQTT_DOMAIN_NAME"]

        self.client.username_pw_set(username, password=password)
        self.client.connect(domain_name, MQTT_PORT)

        self.client.loop_forever()
