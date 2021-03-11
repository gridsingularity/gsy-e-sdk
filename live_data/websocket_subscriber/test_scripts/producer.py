

import asyncio
import websockets
import json
from random import randint


async def produce(message: str, host: str, port: int) -> None:
    async with websockets.connect(f"ws://{host}:{port}") as ws:
        await ws.send(message)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    data = {'area_name': 'OLI_6', 'energy_wh': randint(0, 100)}
    forecast_message = json.dumps(
        {'event': 'live_data', 'data': data}).\
        encode('utf-8')
    loop.run_until_complete(
        produce(forecast_message, host='localhost', port=4000))
