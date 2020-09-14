import logging
import json
import traceback
from slugify import slugify

from redis import StrictRedis
from concurrent.futures.thread import ThreadPoolExecutor

from d3a_interface.utils import wait_until_timeout_blocking
from d3a_api_client.constants import MAX_WORKER_THREADS


class RedisMarketClient:
    def __init__(self, area_id, redis_url='redis://localhost:6379'):
        self.area_id = slugify(area_id, to_lower=True)
        self.redis_db = StrictRedis.from_url(redis_url)
        self.pubsub = self.redis_db.pubsub()
        self._subscribe_to_response_channels()
        self._blocking_command_responses = {}
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)

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
                        for command_name in ["market_stats", "grid_fees", "dso_market_stats"]}
        channel_subs.update({
            f'{self.area_id}/market-events/market': self._on_market_cycle,
            f'{self.area_id}/events/finish': self._on_finish
        })
        self.pubsub.subscribe(**channel_subs)
        self.pubsub.run_in_thread(daemon=True)

    def _wait_and_consume_command_response(self, command_type):
        logging.debug(f"Command {command_type} waiting for response...")
        wait_until_timeout_blocking(lambda: command_type in self._blocking_command_responses, timeout=120)
        command_output = self._blocking_command_responses.pop(command_type)
        logging.debug(f"Command {command_type} got response {command_output}")
        return command_output

    def list_market_stats(self, market_slot_list):
        logging.debug(f"Client tries to read market_stats.")
        self.redis_db.publish(f"{self.area_id}/market_stats", json.dumps({"market_slots": market_slot_list}))
        return self._wait_and_consume_command_response("market_stats")

    def grid_fees(self, fee_cents_kwh):
        logging.debug(f"Client tries to change grid fees.")
        self.redis_db.publish(f"{self.area_id}/grid_fees", json.dumps({"fee_const": fee_cents_kwh}))
        return self._wait_and_consume_command_response("grid_fees")

    def change_grid_fees_percent(self, fee_percent):
        logging.debug(f"Client tries to change grid fees.")
        self.redis_db.publish(f"{self.area_id}/grid_fees", json.dumps({"fee_percent": fee_percent}))
        return self._wait_and_consume_command_response("grid_fees")

    def list_dso_market_stats(self, market_slot_list):
        logging.debug(f"Client tries to read dso_market_stats.")
        self.redis_db.publish(f"{self.area_id}/dso_market_stats", json.dumps({"market_slots": market_slot_list}))
        return self._wait_and_consume_command_response("dso_market_stats")

    def _on_market_cycle(self, msg):
        message = json.loads(msg["data"])
        logging.info(f"A new market was created. Market information: {message}")

        def executor_function():
            try:
                self.on_market_cycle(message)
            except Exception as e:
                logging.error(f"on_market_cycle raised exception "
                              f"(market_uuid: {message['market_info']['id']}): {e}."
                              f" \n Traceback: {traceback.format_exc()}")
        self.executor.submit(executor_function)

    def on_market_cycle(self, market_info):
        pass

    def _on_finish(self, msg):
        message = json.loads(msg["data"])
        logging.info(f"Simulation finished. Information: {message}")

        def executor_function():
            try:
                self.on_finish(message)
            except Exception as e:
                logging.error(f"on_finish raised exception "
                              f"(market_uuid: {message['market_info']['id']}): {e}."
                              f" \n Traceback: {traceback.format_exc()}")
        self.executor.submit(executor_function)

    def on_finish(self, finish_info):
        pass
