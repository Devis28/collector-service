import requests
import asyncio
import websockets
import json
import uuid
from datetime import datetime

SONG_API = "https://radio-melody-api.fly.dev/song"
LISTENERS_WS = "wss://radio-melody-api.fly.dev/ws/listeners"

def get_current_song():
    r = requests.get(SONG_API)
    data = r.json()
    required_fields = ["station", "title", "artist", "date", "time", "last_update"]
    valid = all(k in data for k in required_fields)
    return {
        "data": data,
        "recorded_at": datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
        "raw_valid": valid,
        "song_session_id": str(uuid.uuid4())
    }

async def get_listeners(song_session_id, interval=30, duration=10*60):
    results = []
    start = datetime.now()
    required_fields = ["last_update", "listeners"]
    async with websockets.connect(LISTENERS_WS) as ws:
        while (datetime.now() - start).total_seconds() < duration:
            msg = await ws.recv()
            data = json.loads(msg)
            valid = all(k in data for k in required_fields)
            results.append({
                "data": data,
                "recorded_at": datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
                "raw_valid": valid,
                "song_session_id": song_session_id
            })
            await asyncio.sleep(interval)
    return results
