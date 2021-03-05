import logging
import traceback
import threading
import asyncio

from concurrent.futures.thread import ThreadPoolExecutor

from d3a_api_client.utils import logging_decorator
from d3a_api_client.websocket_device import WebsocketMessageReceiver
from d3a_api_client.constants import MAX_WORKER_THREADS
from d3a_api_client.websocket_device import retry_coroutine
from d3a_api_client.utils import RestCommunicationMixin
from d3a_api_client.utils import retrieve_jwt_key_from_server


class LiveDataWebsocketMessageReceiver(WebsocketMessageReceiver):
    def __init__(self, rest_client):
        super().__init__(rest_client)

    def _handle_event_message(self, message):
        if message["event"] == "live_data":
            self.client.set_energy_forecast(**message['data'])
        else:
            logging.error(f"Received message with unknown event type: {message}")

    def received_message(self, message):
        try:
            if "event" in message:
                self._handle_event_message(message)
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


class LiveData(RestCommunicationMixin):

    def __init__(self, domain_name, websockets_domain_name):

        self.domain_name = domain_name
        self.websockets_domain_name = websockets_domain_name
        self.jwt_token = retrieve_jwt_key_from_server(self.domain_name)
        self._create_jwt_refresh_timer(self.domain_name)
        self.start_websocket_connection()

    def start_websocket_connection(self):
        self.dispatcher = LiveDataWebsocketMessageReceiver(self)
        self.websocket_thread = LiveDataWebsocketThread(
            self.websockets_domain_name, self.domain_name, self.dispatcher
        )
        self.websocket_thread.start()
        self.callback_thread = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)

    @logging_decorator('set_energy_forecast')
    def set_energy_forecast(self, *args, **kwargs):
        self.simulation_id = kwargs['simulation_id']
        self.device_id = kwargs['area_uuid']
        transaction_id, posted = self._post_request('set_energy_forecast',
                                                    {"energy_forecast": kwargs['energy_wh']})
