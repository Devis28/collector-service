import requests
import websockets
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid
import time as time_module

SONG_API = "http://hron.fei.tuke.sk:8152/song"
LISTENERS_WS = "ws://hron.fei.tuke.sk:8152/ws/listeners"

last_successful_listeners = None
last_listeners_update = 0
LAST_RAW_LISTENERS = None
LAST_RAW_LISTENERS_TS = None
LISTENERS_CACHE_TIME = 30  # 5 minút

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]\t{session_part} {text}")

def is_valid_song(data):
    # platí iba ak presne tieto a žiadne iné kľúče!
    return (
        isinstance(data, dict)
        and set(data.keys()) == {"song", "artist", "start_time"}
        and all(data[k] is not None for k in ["song", "artist", "start_time"])
    )

def flatten_song(song_obj):
    raw = song_obj.get("raw", {})
    flat = dict(raw)
    flat["recorded_at"] = song_obj["recorded_at"]
    flat["raw_valid"] = song_obj["raw_valid"]
    flat["song_session_id"] = song_obj["song_session_id"]
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
        log_radio_event("VLNA", f"Chyba pri získavaní skladby: {e}")
        return {
            "raw": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }

def is_valid_listeners(data):
    return (
        isinstance(data, dict)
        and set(data.keys()) == {"listeners", "timestamp"}
        and isinstance(data["listeners"], int)
        and isinstance(data["timestamp"], str)
    )

def flatten_listener(listener_obj):
    raw = listener_obj.get("raw", {})
    flat = dict(raw)
    flat["recorded_at"] = listener_obj["recorded_at"]
    flat["raw_valid"] = listener_obj["raw_valid"]
    flat["song_session_id"] = listener_obj["song_session_id"]
    return flat

async def try_get_listeners_once():
    global last_successful_listeners, last_listeners_update, LAST_RAW_LISTENERS, LAST_RAW_LISTENERS_TS
    try:
        async with websockets.connect(LISTENERS_WS) as websocket:
            raw = await asyncio.wait_for(websocket.recv(), timeout=30.0)
            data = json.loads(raw)
            raw_valid = is_valid_listeners(data)
            LAST_RAW_LISTENERS = data
            LAST_RAW_LISTENERS_TS = datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S")

            if raw_valid:
                last_successful_listeners = data["listeners"]
                last_listeners_update = time_module.time()
                log_radio_event("VLNA", f"Úspešne získané dáta o poslucháčoch: {last_successful_listeners}")
            else:
                log_radio_event("VLNA", f"Nesprávna štruktúra listeners {[k for k in data]}")
            return data
    except Exception as e:
        log_radio_event("VLNA", f"Chyba pri pokuse o listeners: {e}")

    return {}

async def get_current_listeners(session_id=None):
    global last_successful_listeners, last_listeners_update, LAST_RAW_LISTENERS, LAST_RAW_LISTENERS_TS
    current_time = time_module.time()

    if (
        LAST_RAW_LISTENERS is not None
        and (current_time - last_listeners_update < LISTENERS_CACHE_TIME)
    ):
        data = LAST_RAW_LISTENERS
        raw_valid = is_valid_listeners(data)
        return {
            "raw": data,
            "recorded_at": LAST_RAW_LISTENERS_TS or datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": raw_valid,
            "song_session_id": session_id
        }

    if current_time - last_listeners_update >= LISTENERS_CACHE_TIME:
        data = await try_get_listeners_once()
        raw_valid = is_valid_listeners(data)
        return {
            "raw": data,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": raw_valid,
            "song_session_id": session_id
        }

    return {
        "raw": {},
        "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
        "raw_valid": False,
        "song_session_id": session_id
    }
