from time import sleep
from d3a_api_client.redis import RedisClient

r = RedisClient('house-2', 'my-load-device', autoregister=False)
r.register(True)
print(f"WAITING")
sleep(3)
print(f"POSTING")
r.bid_energy(2, 3)
print(f"FINISHED")
