import asyncio
import logging
import threading
import traceback
import websockets
import json
from d3a_interface.utils import wait_until_timeout_blocking


class WebsocketMessageReceiver:
    def __init__(self, rest_client):
        self.client = rest_client
        self.command_response_buffer = []

    def received_message(self, message):
        if "event" in message:
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
        elif "command" in message:
            self.command_response_buffer.append(message)

    def wait_for_command_response(self, command_name, transaction_id):
        def check_if_command_response_received():
            return any("command" in c and c["command"] == command_name and
                       "transaction_id" in c and c["transaction_id"] == transaction_id
                       for c in self.command_response_buffer)

        logging.info(f"Command {command_name} waiting for response...")
        wait_until_timeout_blocking(check_if_command_response_received, timeout=120)
        response = next(c
                        for c in self.command_response_buffer
                        if "command" in c and c["command"] == command_name and
                        "transaction_id" in c and c["transaction_id"] == transaction_id)
        self.command_response_buffer.remove(response)
        return response


async def websocket_coroutine(websocket_uri, websocket_headers, message_dispatcher):
    async with websockets.connect(websocket_uri, extra_headers=websocket_headers) as websocket:
        while True:
            try:
                message = await websocket.recv()
                logging.debug(f"Websocket received message {message}")
                message_dispatcher.received_message(json.loads(message.decode('utf-8')))
            except Exception as e:
                raise Exception(f"Error while receiving message: {str(e)}, "
                                f"traceback:{traceback.format_exc()}")


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
            websocket_coroutine(websockets_uri, self.websocket_headers, self.message_dispatcher)
        )
        event_loop.close()
