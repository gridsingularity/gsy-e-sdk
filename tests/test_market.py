"""
 Hello!

    Your connection request to "Area 2" with uuid: "925f9307-7228-4f89-9178-9dfdbcda2c24"
    (collaboration: {'name': '2158', 'uuid': '95df55f0-2ec9-4932-9449-e3302405ab5f'})
    has been approved.



    ----- d3a.io -----
"""

from d3a_api_client.rest_market import RestMarketClient
from d3a_api_client.redis_market import RedisMarketClient
from time import sleep


# class TestMarketClient(RedisMarketClient):
#     def on_market_cycle(self, market_info):
#         # print(market_info)
#         print("trying")
#         print(self.list_dso_market_stats(["2020-03-24T00:00", "2020-03-24T00:15"]))
# #
# #
# #
# market_connection = TestMarketClient("house-2")
#
#
# while True:
#     sleep(0.5)


class TestMarketClient(RestMarketClient):

    def on_market_cycle(self, market_info):
        print(market_info)
        # print("trying")
        # print(self.list_dso_market_stats(["2020-03-24T00:00", "2020-03-24T00:15"]))

    # def on_finish(self, finish_info):
    #     print("finished")
    #     print(finish_info)

from d3a_api_client.utils import get_area_uuid_from_area_name_and_collaboration_id
import os

# os.environ['API_CLIENT_USERNAME'] = "hannes@gridsingularity.com"
os.environ['API_CLIENT_USERNAME'] = "dso@dso.com"
os.environ['API_CLIENT_PASSWORD'] = "d3a_sim_hd"
collab_id = "95df55f0-2ec9-4932-9449-e3302405ab5f"
# device_name = "Load"
# domain_name = 'https://d3aweb-dev.gridsingularity.com'
# wss_domain = 'wss://d3aweb-dev.gridsingularity.com/external-ws'
domain_name = 'http://localhost:8000'
wss_domain = 'ws://localhost:8000/external-ws'
# area_uuid = get_area_uuid_from_area_name_and_collaboration_id(collab_id, device_name, domain_name)
# Connects one client to the load device


load = TestMarketClient(
    simulation_id=collab_id,
    area_id="925f9307-7228-4f89-9178-9dfdbcda2c24",
    domain_name=domain_name,
    websockets_domain_name=wss_domain)



# Infinite loop in order to leave the client running on the background
# placing bids and offers on every market cycle.
while True:
    print("list_dso_market_stats")
    print(load.list_dso_market_stats(["2020-03-24T00:00", "2020-03-24T00:15"]))
    print("ready")
