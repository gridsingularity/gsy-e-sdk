import os
import logging
import traceback
import threading
import asyncio

from concurrent.futures.thread import ThreadPoolExecutor

from d3a_api_client.utils import consumer_websocket_domain_name_from_env, domain_name_from_env, \
    retrieve_jwt_key_from_server, RestCommunicationMixin
from d3a_api_client.websocket_device import WebsocketMessageReceiver
from d3a_api_client.constants import MAX_WORKER_THREADS
from d3a_api_client.websocket_device import retry_coroutine
from d3a_api_client.rest_device import RestDeviceClient


class LiveDataWebsocketMessageReceiver(WebsocketMessageReceiver):

    def received_message(self, message):
        try:
            if "event" in message and message["event"] == "live_data_subscriber":
                dev_idf = message['data']['device_identifier']
                for api_args in self.client.device_api_client_mapping[dev_idf]:
                    RestDeviceClient(**api_args).set_energy_forecast(
                        message['data']['energy_wh'], do_not_wait=True
                    )
        except Exception as e:
            logging.error(f"Error while processing incoming message {message}. Exception {e}.\n"
                          f"{traceback.format_exc()}")


class LiveDataWebsocketThread(threading.Thread):

    def __init__(self, websocket_domain_name, http_domain_name, dispatcher, *args, **kwargs):
        self.domain_name = websocket_domain_name
        self.http_domain_name = http_domain_name
        self.message_dispatcher = dispatcher
        super().__init__(*args, **kwargs, daemon=True)

    def run(self):
        event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(event_loop)
        event_loop.run_until_complete(
            retry_coroutine(self.domain_name, self.http_domain_name, self.message_dispatcher)
        )
        event_loop.close()


class WSConsumer(RestCommunicationMixin):

    def __init__(self, device_api_client_mapping):
        super().__init__()
        self.domain_name = domain_name_from_env
        self.jwt_token = retrieve_jwt_key_from_server(self.domain_name)
        self._create_jwt_refresh_timer(self.domain_name)
        self.device_api_client_mapping = device_api_client_mapping
        self.websockets_domain_name = consumer_websocket_domain_name_from_env
        self.start_websocket_connection()

    def start_websocket_connection(self):
        self.dispatcher = LiveDataWebsocketMessageReceiver(self)
        self.websocket_thread = LiveDataWebsocketThread(
            self.websockets_domain_name, self.domain_name, self.dispatcher
        )
        self.websocket_thread.start()
        self.callback_thread = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)

