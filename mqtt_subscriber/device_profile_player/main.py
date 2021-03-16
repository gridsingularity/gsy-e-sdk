import os
import logging
import json
import asyncio
from time import time
import websockets
from pendulum import now, duration
from d3a_interface.utils import from_timestamp
from d3a_interface.read_user_profile import read_profile_without_config


class DeviceProfilePlayback:

    def __init__(self):
        profile_path = os.environ['DEVICE_PLAYBACK_PROFILES_PATH']
        self._year_to_replay = int(os.environ['DEVICE_PLAYBACK_YEAR_TO_REPLAY'])
        csv_files = self._list_all_available_filenames_and_paths(profile_path)
        self.profile_dicts = {
            filename.split(".")[0]: read_profile_without_config(filepath)
            for filename, filepath in csv_files
        }

    @staticmethod
    def _list_all_available_filenames_and_paths(profile_path):
        filenames = os.listdir(profile_path)
        return [(filename, os.path.join(profile_path, filename))
                for filename in filenames
                if filename.endswith(".csv")]

    async def _scheduled_call(self, websocket):
        current_timestamp = now().timestamp()
        offset_seconds = current_timestamp % 900
        requested_datetime = from_timestamp(current_timestamp - offset_seconds)\
            .replace(year=self._year_to_replay)

        for device_name, device_profile in self.profile_dicts.items():
            logging.info(f"Sending energy {device_profile[requested_datetime]} to device {device_name}.")
            data_to_send = {
                "energy_requirement_kWh": device_profile[requested_datetime],
                "device_id": device_name
            }
            await websocket.send(json.dumps(data_to_send))

    async def sleep_until(self, resume_at):
        await asyncio.sleep((resume_at - now()).total_seconds())

    async def run(self, websocket, _):
        while True:
            next_cycle_datetime = now() + duration(seconds=900)
            await self._scheduled_call(websocket)
            await self.sleep_until(next_cycle_datetime)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    logging.info(f"Starting the profile reading.")
    t1 = time()
    device_profile_playback = DeviceProfilePlayback()
    t2 = time()
    logging.info(f"Profile reading lasted {t2 - t1} seconds.")
    start_server = websockets.serve(device_profile_playback.run, "localhost", 8765)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()

