import logging
import sys

import gsy_e_sdk

logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logger.addHandler(console_handler)

logger.warning(
    "The d3a_api_client package name will be deprecated soon. Please use gsy_e_sdk instead.")

__all__ = ["gsy_e_sdk"]

sys.modules[__name__] = sys.modules["gsy_e_sdk"]
