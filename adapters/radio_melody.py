import requests
import websockets
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo

SONG_API = "https://radio-melody-api.fly.dev/song"
LISTENERS_WS = "wss://radio-melody-api.fly.dev/ws/listeners"

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]{session_part} {text}")

def flatten_song(song_obj, session_id):
    result = dict(song_obj["data"])
    result["recorded_at"] = song_obj.get("recorded_at")
    result["raw_valid"] = song_obj.get("raw_valid")
    result["song_session_id"] = session_id
    return result

def flatten_listener(listener_obj, session_id):
    result = dict(listener_obj.get("data", {}))
    result["recorded_at"] = listener_obj.get("recorded_at")
    result["raw_valid"] = listener_obj.get("raw_valid")
    result["song_session_id"] = session_id
    return result

def get_current_song():
    try:
        r = requests.get(SONG_API)
        data = r.json()
        required_fields = ["station", "title", "artist", "date", "time", "last_update"]
        valid = all(k in data for k in required_fields)
        return {
            "data": data,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": valid
        }
    except Exception as e:
        return {
            "data": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False
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

def log_cloudflare_upload(radio_name, r2_path):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    print(f"[{timestamp}] [{radio_name}] Dáta nahrané do Cloudflare: {r2_path}")
