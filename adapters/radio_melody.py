import requests
import websockets
import asyncio
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
        radio_name = data.get("station", "melody")
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
        log_radio_event("melody", f"Chyba pri získavaní skladby: {e}", session_id)
        return {
            "data": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": session_id
        }

async def get_current_listeners():
    listeners_data = {}
    session = await websockets.connect(LISTENERS_WS)
    try:
        msg = await asyncio.wait_for(session.recv(), timeout=10)
        data = json.loads(msg)
        required_fields = ["last_update", "listeners"]
        valid = all(k in data for k in required_fields)
        listeners_data = {
            "data": data,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": valid
        }
    except Exception as e:
        listeners_data = {"error": str(e)}
    finally:
        await session.close()
    return listeners_data
