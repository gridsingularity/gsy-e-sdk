from time import sleep
from d3a_api_client.redis_device import RedisDeviceClient


class TestClient(RedisDeviceClient):
    def on_market_cycle(self, market_info):
        print(f"market_info: {market_info}")
        # print(f"BID: {r.bid_energy(1, 33)}")
        # bid_listing = self.list_bids()
        # print(f"bid_listing: {bid_listing}")
        print(f"OFFER: {r.offer_energy(1, 10)}")
        offer_listing = self.list_offers()
        print(f"offer_listing: {offer_listing}")


r = TestClient('storage', autoregister=True)
# print(f"WAITING")
# sleep(15)
# print(f"POSTING")
# print(r.bid_energy(0.1, 30))
# print(f"FINISHED")

while True:
    sleep(1)
