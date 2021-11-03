import os
import json
import sys
from gsy_e_sdk.utils import get_area_uuid_and_name_mapping_from_simulation_id

# set login information for d3a web
os.environ["API_CLIENT_USERNAME"] = str(sys.argv[1])
os.environ["API_CLIENT_PASSWORD"] = str(sys.argv[2])

# set simulation parameters
simulation_id = str(sys.argv[3])

mapping = get_area_uuid_and_name_mapping_from_simulation_id(simulation_id)

with open(os.path.join(os.getcwd(), 'area_name_uuid_map.json'), "w") as outfile:
    json.dump(mapping, outfile, indent=2)
