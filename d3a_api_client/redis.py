import logging
import json
from enum import Enum
from functools import wraps
from redis import StrictRedis
from d3a_interface.utils import wait_until_timeout_blocking


class RedisAPIException(Exception):
    pass


def registered_connection(f):
    @wraps(f)
    def wrapped(self, *args, **kwargs):
        if not self.is_active:
            raise RedisAPIException(f'Registration has not completed yet.')
        return f(self, *args, **kwargs)
    return wrapped


class Commands(Enum):
    OFFER = 1
    BID = 2
    DELETE_OFFER = 3
    DELETE_BID = 4
    LIST_OFFERS = 5
    LIST_BIDS = 6


class RedisClient:
    def __init__(self, market_id, client_id, autoregister=True, redis_url='redis://localhost'):
        self.redis_db = StrictRedis.from_url(redis_url)
        self.pubsub = self.redis_db.pubsub()
        self.market_id = market_id
        self.client_id = client_id
        self.is_active = False
        self._blocking_command_responses = {}
        self._subscribe_to_response_channels()
        if autoregister:
            self.register(is_blocking=False)

    def _subscribe_to_response_channels(self):
        command_response_subscriptions = {
            f"{self._command_topics[k]}/response": self._generate_command_response_callback(k)
            for k in Commands
        }
        command_response_subscriptions[f'{self.market_id}/register_participant/response'] = self._on_register
        command_response_subscriptions[f'{self._channel_prefix}/market_cycle'] = self._on_market_cycle
        self.pubsub.subscribe(**command_response_subscriptions)
        self.pubsub.run_in_thread(daemon=True)

    def register(self, is_blocking=False):
        self.redis_db.publish(f'{self.market_id}/register_participant', json.dumps({"name": self.client_id}))
        if is_blocking:
            wait_until_timeout_blocking(lambda: self.is_active, timeout=30)

    @property
    def _channel_prefix(self):
        return f"{self.market_id}/{self.client_id}"

    @property
    def _command_topics(self):
        return {
            Commands.OFFER: f'{self._channel_prefix}/offer',
            Commands.BID: f'{self._channel_prefix}/bid',
            Commands.DELETE_OFFER: f'{self._channel_prefix}/delete_offer',
            Commands.DELETE_BID: f'{self._channel_prefix}/delete_bid',
            Commands.LIST_OFFERS: f'{self._channel_prefix}/offers',
            Commands.LIST_BIDS: f'{self._channel_prefix}/bids'
        }

    def wait_and_consume_command_response(self, command_type):
        wait_until_timeout_blocking(lambda: command_type in self._blocking_command_responses)
        command_output = self._blocking_command_responses.pop(command_type)
        return command_output

    def _generate_command_response_callback(self, command_type):
        def command_received(msg):
            message = json.loads(msg["data"])
            if 'error' in message:
                logging.error(f"Error when receiving {command_type} command response."
                              f"Error output: {message}")
                return
            else:
                self._blocking_command_responses[command_type] = message
        return command_received

    @registered_connection
    def offer_energy(self, energy, price):
        self.redis_db.publish(
            self._command_topics[Commands.OFFER],
            json.dumps({"energy": energy, "price": price})
        )
        return self.wait_and_consume_command_response(Commands.OFFER)

    @registered_connection
    def bid_energy(self, energy, price):
        self.redis_db.publish(
            self._command_topics[Commands.BID],
            json.dumps({"energy": energy, "price": price})
        )
        return self.wait_and_consume_command_response(Commands.BID)

    @registered_connection
    def delete_offer(self, offer_id):
        self.redis_db.publish(
            self._command_topics[Commands.DELETE_OFFER],
            json.dumps({"offer": offer_id})
        )
        return self.wait_and_consume_command_response(Commands.DELETE_OFFER)

    @registered_connection
    def delete_bid(self, bid_id):
        self.redis_db.publish(
            self._command_topics[Commands.DELETE_BID],
            json.dumps({"bid": bid_id})
        )
        return self.wait_and_consume_command_response(Commands.DELETE_BID)

    @registered_connection
    def list_offers(self):
        self.redis_db.publish(self._command_topics[Commands.LIST_OFFERS], json.dumps(""))
        return self.wait_and_consume_command_response(Commands.LIST_OFFERS)

    @registered_connection
    def list_bids(self):
        self.redis_db.publish(self._command_topics[Commands.LIST_BIDS], json.dumps(""))
        return self.wait_and_consume_command_response(Commands.LIST_BIDS)

    def _on_register(self, msg):
        message = json.loads(msg["data"])
        if 'available_publish_channels' not in message or \
                'available_subscribe_channels' not in message:
            raise RedisAPIException(f'Registration to the market {self.market_id} failed.')

        logging.info(f"Client was registered to market: {message}")
        self.is_active = True
        self.on_register()

    def _on_market_cycle(self, msg):
        message = json.loads(msg["data"])
        logging.info(f"A new market was created. Market information: {message}")
        self.on_market_cycle(message)

    def on_register(self):
        pass

    def on_market_cycle(self, market_info):
        pass
