import asyncio
import logging
import threading
import traceback
import websockets
import json
from time import time
from d3a_interface.utils import wait_until_timeout_blocking
from d3a_api_client.constants import WEBSOCKET_ERROR_THRESHOLD_SECONDS, WEBSOCKET_MAX_CONNECTION_RETRIES, \
    WEBSOCKET_WAIT_BEFORE_RETRY_SECONDS


class WebsocketMessageReceiver:
    def __init__(self, rest_client):
        self.client = rest_client
        self.command_response_buffer = []

    def _handle_event_message(self, message):
        if message["event"] == "market":
            self.client._on_market_cycle(message)
        elif message["event"] == "tick":
            self.client._on_tick(message)
        elif message["event"] == "trade":
            self.client._on_trade(message)
        elif message["event"] == "finish":
            self.client._on_finish(message)
        else:
            logging.error(f"Received message with unknown event type: {message}")

    def received_message(self, message):
        try:
            if "event" in message:
                self._handle_event_message(message)
            elif "command" in message:
                self.command_response_buffer.append(message)

            if "event" in message or "command" in message:
                self.client._on_event_or_response(message)
        except Exception as e:
            logging.error(f"Error while processing incoming message {message}. Exception {e}.\n"
                          f"{traceback.format_exc()}")

    def wait_for_command_response(self, command_name, transaction_id, timeout=120):
        def check_if_command_response_received():
            return any("command" in c and c["command"] == command_name and
                       "transaction_id" in c and c["transaction_id"] == transaction_id
                       for c in self.command_response_buffer)

        logging.debug(f"Command {command_name} waiting for response...")
        wait_until_timeout_blocking(check_if_command_response_received, timeout=timeout)
        response = next(c
                        for c in self.command_response_buffer
                        if "command" in c and c["command"] == command_name and
                        "transaction_id" in c and c["transaction_id"] == transaction_id)
        self.command_response_buffer.remove(response)
        return response


async def websocket_coroutine(websocket_uri, websocket_headers, message_dispatcher):
    websocket = await websockets.connect(websocket_uri, extra_headers=websocket_headers)
    while True:
        try:
            message = await websocket.recv()
            logging.debug(f"Websocket received message {message}")
            message_dispatcher.received_message(json.loads(message.decode('utf-8')))
        except Exception as e:
            await websocket.close()
            raise Exception(f"Error while receiving message: {str(e)}, "
                            f"traceback:{traceback.format_exc()}")


async def retry_coroutine(websocket_uri, websocket_headers, message_dispatcher, retry_count=0):
    ws_connect_time = time()
    try:
        await websocket_coroutine(websocket_uri, websocket_headers, message_dispatcher)
    except Exception as e:
        logging.warning(f"Connection failed, trying to reconnect.")
        ws_error_time = time()
        if ws_error_time - ws_connect_time > WEBSOCKET_ERROR_THRESHOLD_SECONDS:
            retry_count = 0
        await asyncio.sleep(WEBSOCKET_WAIT_BEFORE_RETRY_SECONDS)
        if retry_count >= WEBSOCKET_MAX_CONNECTION_RETRIES:
            raise e
        await retry_coroutine(websocket_uri, websocket_headers, message_dispatcher, retry_count=retry_count+1)


class WebsocketThread(threading.Thread):

    def __init__(self, sim_id, area_uuid, jwt_token, domain_name, dispatcher, *args, **kwargs):
        self.message_dispatcher = dispatcher
        self.websocket_headers = {
            "Authorization": f"JWT {jwt_token}"
        }
        self.domain_name = domain_name
        self.sim_id = sim_id
        self.area_uuid = area_uuid
        super().__init__(*args, **kwargs, daemon=True)

    def run(self):
        event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(event_loop)
        websockets_uri = f"{self.domain_name}/{self.sim_id}/{self.area_uuid}/"
        event_loop.run_until_complete(
            retry_coroutine(websockets_uri, self.websocket_headers, self.message_dispatcher)
        )
        event_loop.close()
