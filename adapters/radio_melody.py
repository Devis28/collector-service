import requests
import websockets
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_API = "https://radio-melody-api.fly.dev/song"
LISTENERS_WS = "wss://radio-melody-api.fly.dev/ws/listeners"

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]{session_part} {text}")

def is_valid_song(data):
    required = {"station", "title", "artist", "date", "time", "last_update"}
    return isinstance(data, dict) and set(data.keys()) == required

def is_valid_listeners(data):
    required = {"last_update", "listeners"}
    return isinstance(data, dict) and set(data.keys()) == required

def flatten_song(song_obj):
    raw = song_obj.get("raw", {})
    session_id = song_obj["song_session_id"]
    flat = dict(raw)
    flat["recorded_at"] = song_obj["recorded_at"]
    flat["raw_valid"] = song_obj["raw_valid"]
    flat["song_session_id"] = session_id
    return flat

def flatten_listener(listener_obj):
    raw = listener_obj.get("raw", {})
    session_id = listener_obj["song_session_id"]
    flat = dict(raw)
    flat["recorded_at"] = listener_obj["recorded_at"]
    flat["raw_valid"] = listener_obj["raw_valid"]
    flat["song_session_id"] = session_id
    return flat

def get_current_song():
    try:
        r = requests.get(SONG_API)
        data = r.json()
        raw_valid = is_valid_song(data)
        session_id = str(uuid.uuid4())
        return {
            "raw": data,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": raw_valid,
            "song_session_id": session_id
        }
    except Exception as e:
        return {
            "raw": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }

async def get_current_listeners(session_id=None):
    listeners_data = {}
    session = await websockets.connect(LISTENERS_WS)
    try:
        msg = await asyncio.wait_for(session.recv(), timeout=10)
        data = json.loads(msg)
        raw_valid = is_valid_listeners(data)
        listeners_data = {
            "raw": data,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": raw_valid,
            "song_session_id": session_id
        }
    except Exception as e:
        listeners_data = {
            "raw": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": session_id
        }
    finally:
        await session.close()
    return listeners_data

def log_cloudflare_upload(radio_name, r2_path):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    print(f"[{timestamp}] [{radio_name}] Dáta nahrané do Cloudflare: {r2_path}")
