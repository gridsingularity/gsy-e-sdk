import uuid
import json
from redis import StrictRedis

from gsy_e_sdk.constants import LOCAL_REDIS_URL


def send_resume_to_simulation():
    """Send resume command to local running simulation via redis."""
    redis_db = StrictRedis.from_url(LOCAL_REDIS_URL)
    redis_db.publish("/resume", json.dumps({"transaction_id": str(uuid.uuid4())}))
