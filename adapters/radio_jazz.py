import threading
import requests
from fastapi import FastAPI, Request
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_API = "http://147.232.40.154:8000/current"

# Thread-safe cache for latest listeners payload from webhook
last_listeners_payload = {}
last_lock = threading.Lock()

app = FastAPI()

@app.post("/callback")
@app.post("/callback-jazz")
async def callback(req: Request):
    data = await req.json()
    now = datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S")
    required = {"timestamp", "listeners", "radio"}
    valid = isinstance(data, dict) and set(data.keys()) == required and isinstance(data["listeners"], int)
    payload = {
        "raw": data,
        "recorded_at": now,
        "raw_valid": valid,
        "song_session_id": None
    }
    with last_lock:
        last_listeners_payload.clear()
        last_listeners_payload.update(payload)
    print(f"[{now}] [JAZZ] Webhook: {data}")
    return {"status": "ok"}

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]{' ' * (8 - len(radio_name))}{session_part} {text}")

def is_valid_song(data):
    song = data.get("song") if isinstance(data, dict) else None
    required = {"play_date", "play_time", "artist", "title"}
    return isinstance(song, dict) and set(song.keys()) == required

def flatten_song(song_obj):
    raw = song_obj.get("raw", {})
    song = raw.get("song", {}) if isinstance(raw.get("song", {}), dict) else {}
    session_id = song_obj["song_session_id"]
    flat = {k: song.get(k) for k in ["play_date", "play_time", "artist", "title"]}
    flat["recorded_at"] = song_obj["recorded_at"]
    flat["raw_valid"] = song_obj["raw_valid"]
    flat["song_session_id"] = session_id
    return flat

def get_current_song():
    try:
        r = requests.get(SONG_API, timeout=10)
        data = r.json()
        raw_valid = is_valid_song(data)
        session_id = str(uuid.uuid4())
        song = data.get("song", {}) if isinstance(data, dict) else {}
        if not raw_valid:
            log_radio_event("JAZZ", f"Song nenašiel požadované polia: {data}", session_id)
        return {
            "raw": data,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": raw_valid,
            "song_session_id": session_id,
            "title": song.get("title"),
            "artist": song.get("artist"),
        }
    except Exception as e:
        log_radio_event("JAZZ", f"Chyba pri získavaní songu: {e}")
        return {
            "raw": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }

def is_valid_listeners(data):
    required = {"timestamp", "listeners", "radio"}
    return isinstance(data, dict) and set(data.keys()) == required and isinstance(data["listeners"], int)

def flatten_listener(listener_obj):
    raw = listener_obj.get("raw", {})
    session_id = listener_obj.get("song_session_id")
    flat = {k: raw.get(k) for k in ["timestamp", "listeners", "radio"]}
    flat["recorded_at"] = listener_obj["recorded_at"]
    flat["raw_valid"] = listener_obj["raw_valid"]
    flat["song_session_id"] = session_id
    return flat

async def get_current_listeners(session_id=None):
    global last_listeners_payload
    with last_lock:
        payload = last_listeners_payload.copy()
    if not payload or not payload.get("raw_valid"):
        log_radio_event("JAZZ", f"Nepodarilo sa načítať poslucháčov, používaj webhook!", session_id)
        return {
            "raw": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": session_id
        }
    payload["song_session_id"] = session_id
    return payload
