import logging
import traceback

from d3a_interface.client_connections.websocket_connection import WebsocketMessageReceiver
from d3a_interface.utils import wait_until_timeout_blocking


class DeviceWebsocketMessageReceiver(WebsocketMessageReceiver):
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
