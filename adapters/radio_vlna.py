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
LISTENERS_CACHE_TIME = 300  # 5 minút

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]{session_part} {text}")

def is_valid_song(data):
    return (
        isinstance(data, dict)
        and "song" in data
        and "artist" in data
        and "start_time" in data
    )

def get_current_song():
    try:
        r = requests.get(SONG_API)
        data = r.json()
        raw_valid = is_valid_song(data)
        session_id = str(uuid.uuid4())
        return {
            "raw": data,  # uschovaj celé RAW
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": raw_valid,
            "song_session_id": session_id
        }
    except Exception as e:
        log_radio_event("VLNA", f"Chyba pri získavaní skladby: {e}")
        return {
            "raw": {},  # nie je čo uložiť
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }

def is_valid_listeners(data):
    return (
        isinstance(data, dict)
        and "listeners" in data
        and "timestamp" in data
    )

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
                return data  # vraciame celé RAW
            else:
                log_radio_event("VLNA", f"Nesprávna štruktúra listeners {[k for k in data]}")
                return data
    except Exception as e:
        log_radio_event("VLNA", f"Chyba pri pokuse o listeners: {e}")

    return {}  # ak žiadne dáta

async def get_current_listeners(session_id=None):
    global last_successful_listeners, last_listeners_update, LAST_RAW_LISTENERS, LAST_RAW_LISTENERS_TS
    current_time = time_module.time()

    # cache pre listeners
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

    # fallback, keď nie je nič
    return {
        "raw": {},
        "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
        "raw_valid": False,
        "song_session_id": session_id
    }
