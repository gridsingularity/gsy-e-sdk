import logging
import traceback

from multiprocessing import Process

from live_data_subscriber.mqtt import generate_api_client_args_mapping
from live_data_subscriber.mqtt.oli_broker import MQTTConnection
from live_data_subscriber.websocket.consumer import WSConsumer


def main():
    logging.getLogger().setLevel(logging.INFO)
    topic_api_client_mapping, device_api_client_mapping = generate_api_client_args_mapping()

    # start WS
    create_process_nonblocking(WSConsumer, device_api_client_mapping)

    # start mqtt
    create_process_nonblocking(MQTTConnection, topic_api_client_mapping)


def create_process_nonblocking(class_name, class_arg):

    def execute_process(class_name, class_arg):
        class_name(class_arg)
    try:
        p = Process(target=execute_process)
        p.start()
    except Exception as e:
        logging.error(f"Subscriber failed with error {e}")
        logging.error(traceback.format_exc())


if __name__ == "__main__":
    main()
