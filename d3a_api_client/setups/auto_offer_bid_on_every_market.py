from time import sleep
from d3a_api_client.redis_client_base import RedisClient


class AutoOfferBidOnMarket(RedisClient):

    def on_market_cycle(self, market_info):
        self.offer_energy(2, 3)
        self.bid_energy(2, 200)


r = AutoOfferBidOnMarket('house-2', 'serifos', True)

# Infinite loop in order to leave the client running on the background
# placing bids and offers on every market cycle.
while True:
    sleep(0.5)
