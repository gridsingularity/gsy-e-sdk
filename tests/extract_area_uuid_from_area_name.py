import os
import json
import sys
from d3a_api_client.utils import retrieve_jwt_key_from_server, \
    get_area_uuid_and_name_mapping_from_simulation_id

# set login information for d3a web
os.environ["API_CLIENT_USERNAME"] = str(sys.argv[1])
os.environ["API_CLIENT_PASSWORD"] = str(sys.argv[2])

# set simulation parameters
simulation_id = str(sys.argv[3])
domain_name = 'https://d3aweb.gridsingularity.com'

mapping = get_area_uuid_and_name_mapping_from_simulation_id(
    simulation_id, domain_name
)

with open(os.path.join(os.getcwd(), 'area_name_uuid_map.csv'), "w") as mapped:
    mapped.write(json.dumps(mapping, indent=2))
