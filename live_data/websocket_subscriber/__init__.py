from live_data.websocket_subscriber.live_data_consumer import LiveDataConsumer


def create_live_data_consumer(device_api_client_mapping):
    print(f"create_live_data_consumer")
    live_data = LiveDataConsumer(device_api_client_mapping)
