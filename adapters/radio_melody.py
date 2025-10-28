import requests
import asyncio
import websockets
import datetime
import uuid
import json

SONG_API = "https://radio-melody-api.fly.dev/song"
LISTENERS_WS = "wss://radio-melody-api.fly.dev/ws/listeners"

def fetch_song():
    response = requests.get(SONG_API)
    data = response.json()
    required = ["station", "title", "artist", "date", "time", "last_update"]
    raw_valid = all(data.get(col) for col in required)
    data["recorded_at"] = datetime.datetime.now().isoformat()
    data["raw_valid"] = raw_valid
    data["song_session_id"] = str(uuid.uuid4()) if raw_valid else None
    return data

async def collect_listeners(song_session_id, duration=30):
    listeners_records = []
    start = datetime.datetime.now()
    async with websockets.connect(LISTENERS_WS) as ws:
        while (datetime.datetime.now() - start).seconds < duration:
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=12)
                info = json.loads(message)
                required = ["listeners", "last_update"]
                raw_valid = all(col in info for col in required)
                info["recorded_at"] = datetime.datetime.now().isoformat()
                info["raw_valid"] = raw_valid
                info["song_session_id"] = song_session_id
                listeners_records.append(info)
            except asyncio.TimeoutError:
                continue  # No message, reconnect or skip
    return listeners_records
