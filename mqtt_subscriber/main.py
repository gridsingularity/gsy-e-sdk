import logging
import traceback
from mqtt_subscriber import generate_topic_api_client_args_mapping
from mqtt_subscriber.oli_broker import MQTTConnection


def main():
    logging.getLogger().setLevel(logging.INFO)
    topic_api_client_mapping = generate_topic_api_client_args_mapping()

    # Connect to the MQTT broker
    mqtt_connection = MQTTConnection(topic_api_client_mapping)
    try:
        mqtt_connection.run_forever()
    except Exception as e:
        logging.error(f"MQTT Subscriber failed with error {e}")
        logging.error(traceback.format_exc())


if __name__ == "__main__":
    main()
