import logging
import json
from redis import StrictRedis
from d3a_interface.utils import wait_until_timeout_blocking


class RedisMarketClient:
    def __init__(self, area_id, redis_url='redis://localhost:6379'):
        self.area_id = area_id
        self.redis_db = StrictRedis.from_url(redis_url)
        self.pubsub = self.redis_db.pubsub()
        self._subscribe_to_response_channels()
        self._blocking_command_responses = {}

    def _generate_command_response_callback(self, command_type):
        def _command_received(msg):
            try:
                message = json.loads(msg["data"])
            except Exception as e:
                logging.error(f"Received incorrect response on command {command_type}. "
                              f"Response {msg}. Error {e}.")
                return
            logging.debug(f"Command {command_type} received response: {message}")
            if 'error' in message:
                logging.error(f"Error when receiving {command_type} command response."
                              f"Error output: {message}")
                return
            else:
                self._blocking_command_responses[command_type] = message
        return _command_received

    def _subscribe_to_response_channels(self):
        channel_subs = {f"{self.area_id}/response/{command_name}":
                        self._generate_command_response_callback(command_name)
                        for command_name in ["market_stats", "grid_fees"]}
        self.pubsub.subscribe(**channel_subs)
        self.pubsub.run_in_thread(daemon=True)

    def _wait_and_consume_command_response(self, command_type):
        logging.info(f"Command {command_type} waiting for response...")
        wait_until_timeout_blocking(lambda: command_type in self._blocking_command_responses, timeout=120)
        command_output = self._blocking_command_responses.pop(command_type)
        logging.info(f"Command {command_type} got response {command_output}")
        return command_output

    def list_market_stats(self, market_slot_list):
        logging.debug(f"Client tries to read market_stats.")
        self.redis_db.publish(f"{self.area_id}/market_stats", json.dumps({"market_slots": market_slot_list}))
        return self._wait_and_consume_command_response("market_stats")

    def change_grid_fees(self, fee_cents_kwh):
        logging.debug(f"Client tries to change grid fees.")
        self.redis_db.publish(f"{self.area_id}/grid_fees", json.dumps({"fee": fee_cents_kwh}))
        return self._wait_and_consume_command_response("grid_fees")
