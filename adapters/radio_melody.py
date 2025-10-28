import requests
import asyncio
import websockets
import json
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo  # od Python 3.9+

SONG_API = "https://radio-melody-api.fly.dev/song"
LISTENERS_WS = "wss://radio-melody-api.fly.dev/ws/listeners"

def log_radio_event(radio_name, text, session_id):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    print(f"[{timestamp}] [{radio_name}] [{session_id}] {text}")

def get_current_song():
    try:
        r = requests.get(SONG_API)
        data = r.json()
        required_fields = ["station", "title", "artist", "date", "time", "last_update"]
        valid = all(k in data for k in required_fields)
        song_title = data.get("title", "")
        radio_name = data.get("station", "Rádio Melody")
        session_id = str(uuid.uuid4())
        log_radio_event(radio_name, f"Zachytená skladba: {song_title}", session_id)
        return {
            "data": data,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": valid,
            "song_session_id": session_id
        }
    except Exception as e:
        session_id = str(uuid.uuid4())
        log_radio_event("Rádio Melody", f"Chyba pri získavaní skladby: {e}", session_id)
        return {
            "data": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": session_id
        }

async def get_listeners(song_session_id, interval=30, duration=10*60):
    results = []
    start = datetime.now(ZoneInfo("Europe/Bratislava"))
    required_fields = ["last_update", "listeners"]
    try:
        async with websockets.connect(LISTENERS_WS) as ws:
            while (datetime.now(ZoneInfo("Europe/Bratislava")) - start).total_seconds() < duration:
                msg = await ws.recv()
                data = json.loads(msg)
                valid = all(k in data for k in required_fields)
                listeners_count = data.get("listeners", 0)
                log_radio_event("Rádio Melody", f"Zachytení poslucháči: {listeners_count}", song_session_id)
                results.append({
                    "data": data,
                    "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                    "raw_valid": valid,
                    "song_session_id": song_session_id
                })
                await asyncio.sleep(interval)
    except Exception as e:
        log_radio_event("Rádio Melody", f"Chyba pri získavaní listeners: {e}", song_session_id)
    return results
