import requests
import websockets
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_API = "https://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/now-playing"
LISTENERS_WS = "wss://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/listeners"

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    radio_fmt = f"{radio_name:<8}"  # zarovnaj všetky rádia na 8 znakov (najdlhší FUNRADIO)
    session_part = f"[{session_id}] " if session_id else ""
    print(f"[{timestamp}] [{radio_fmt}] {session_part}{text}")

def is_valid_song(data):
    wanted = {"radio", "interpreters", "title", "start_time", "timestamp"}
    return (
        isinstance(data, dict)
        and set(data.keys()) == wanted
        and all(data[k] is not None for k in wanted)
    )

def is_valid_song_idle(data):
    return (
        isinstance(data, dict)
        and data.get("is_playing") is False
        and "message" in data
        and "radio" in data
        and "timestamp" in data
        and len(data.keys()) == 4
    )

def is_valid_listeners(data):
    return (
        isinstance(data, dict)
        and set(data.keys()) == {"listeners", "timestamp"}
        and isinstance(data["listeners"], int)
        and isinstance(data["timestamp"], str)
    )

def flatten_song(song_obj):
    raw = song_obj.get("raw", {})
    flat = dict(raw)
    flat["recorded_at"] = song_obj["recorded_at"]
    flat["raw_valid"] = song_obj["raw_valid"]
    flat["song_session_id"] = song_obj["song_session_id"]
    return flat

def flatten_listener(listener_obj):
    raw = listener_obj.get("raw", {})
    flat = dict(raw)
    flat["recorded_at"] = listener_obj["recorded_at"]
    flat["raw_valid"] = listener_obj["raw_valid"]
    flat["song_session_id"] = listener_obj["song_session_id"]
    return flat

def get_current_song():
    try:
        r = requests.get(SONG_API)
        data = r.json()
        session_id = str(uuid.uuid4())
        rec_at = datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S")

        if is_valid_song(data):
            return {
                "raw": data,
                "recorded_at": rec_at,
                "raw_valid": True,
                "song_session_id": session_id
            }
        elif is_valid_song_idle(data):
            return {
                "raw": data,
                "recorded_at": rec_at,
                "raw_valid": True,
                "song_session_id": session_id
            }
        else:
            return {
                "raw": data,
                "recorded_at": rec_at,
                "raw_valid": False,
                "song_session_id": session_id
            }
    except Exception as e:
        log_radio_event("BETA", f"Chyba pri získavaní skladby: {e}")
        return {
            "raw": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }

async def try_get_listeners_once():
    try:
        async with websockets.connect(LISTENERS_WS) as websocket:
            raw = await asyncio.wait_for(websocket.recv(), timeout=30.0)
            data = json.loads(raw)
            if is_valid_listeners(data):
                log_radio_event("BETA", f"Úspešne získané dáta o poslucháčoch: {data['listeners']}")
            else:
                log_radio_event("BETA", f"Nesprávne listeners: {data}")
            return data
    except Exception as e:
        log_radio_event("BETA", f"Chyba pri pokuse o listeners: {e}")
        return {}

def get_current_listeners(session_id=None):
    data = asyncio.run(try_get_listeners_once())
    raw_valid = is_valid_listeners(data)
    return {
        "raw": data,
        "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
        "raw_valid": raw_valid,
        "song_session_id": session_id
    }
