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
LISTENERS_CACHE_TIME = 300  # 5 minút

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]{session_part} {text}")

def get_current_song():
    try:
        r = requests.get(SONG_API)
        data = r.json()
        if "song" in data and "artist" in data:
            transformed_data = {
                "song": {
                    "musicAuthor": data.get("artist"),
                    "musicTitle": data.get("song"),
                    "radio": "Vlna",
                    "startTime": data.get("start_time")
                },
                "last_update": data.get("start_time"),
                "raw_valid": True
            }
        else:
            transformed_data = {
                "song": {
                    "musicAuthor": None,
                    "musicTitle": None,
                    "radio": "Vlna",
                    "startTime": None
                },
                "last_update": data.get("start_time"),
                "raw_valid": False
            }
        session_id = str(uuid.uuid4())
        return {
            **transformed_data,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "song_session_id": session_id
        }
    except Exception as e:
        log_radio_event("VLNA", f"Chyba pri získavaní skladby: {e}")
        return {
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }

async def try_get_listeners_once():
    global last_successful_listeners, last_listeners_update
    try:
        async with websockets.connect(LISTENERS_WS) as websocket:
            raw = await asyncio.wait_for(websocket.recv(), timeout=30.0)
            data = json.loads(raw)
            if "listeners" in data:
                last_successful_listeners = data["listeners"]
                last_listeners_update = time_module.time()
                log_radio_event("VLNA", f"Úspešne získané dáta o poslucháčoch: {last_successful_listeners}")
                return last_successful_listeners
    except asyncio.TimeoutError:
        log_radio_event("VLNA", "Timeout pri čakaní na dáta o poslucháčoch")
    except websockets.exceptions.InvalidStatusCode as e:
        if e.status_code == 429:
            log_radio_event("VLNA", "HTTP 429 - Príliš veľa požiadaviek, skúste neskôr")
        else:
            log_radio_event("VLNA", f"WebSocket chyba: {e}")
    except Exception as e:
        log_radio_event("VLNA", f"Chyba pri pokuse o listeners: {e}")
    return None

async def get_current_listeners(session_id=None):
    global last_successful_listeners, last_listeners_update
    current_time = time_module.time()
    if (last_successful_listeners is not None and
            current_time - last_listeners_update < LISTENERS_CACHE_TIME):
        return {
            "listeners": last_successful_listeners,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": True,
            "song_session_id": session_id
        }
    if current_time - last_listeners_update >= LISTENERS_CACHE_TIME:
        listeners = await try_get_listeners_once()
        if listeners is not None:
            return {
                "listeners": listeners,
                "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                "raw_valid": True,
                "song_session_id": session_id
            }
    return {
        "listeners": None,
        "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
        "raw_valid": False,
        "song_session_id": session_id
    }
