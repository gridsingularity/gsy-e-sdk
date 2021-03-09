import logging
import traceback
from mqtt_subscriber import generate_topic_api_client_args_mapping
from mqtt_subscriber.oli_broker import MQTTConnection
from multiprocessing import Process
from websocket_subscriber import create_live_data_consumer

def main():
    logging.getLogger().setLevel(logging.INFO)
    topic_api_client_mapping = generate_topic_api_client_args_mapping()

    # Connect to the MQTT broker
    try:
        p1 = Process(target=create_live_data_consumer())
        p1.start()

        p2 = Process(target=MQTTConnection(topic_api_client_mapping).run_forever())
        p2.start()


    except Exception as e:
        logging.error(f"MQTT Subscriber failed with error {e}")
        logging.error(traceback.format_exc())


if __name__ == "__main__":
    main()
