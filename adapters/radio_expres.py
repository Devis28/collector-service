import threading
import requests
import json
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, request

SONG_FILE = "/tmp/expres_last_song.json"  # cesta, kam si dočasne uložíš posledný payload

LISTENERS_API = "http://147.232.205.56:5010/api/current_listeners"

app = Flask(__name__)
latest_song = {"data": {}, "timestamp": None, "raw_valid": False, "song_session_id": None}

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]{session_part} {text}")

@app.route("/expres_webhook", methods=["POST"])
def expres_webhook():
    data = request.json
    # naplň štruktúru
    session_id = str(uuid.uuid4())
    entry = {
        "data": data,
        "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
        "raw_valid": set(data) >= {"song", "artists", "isrc", "start_time", "radio"},
        "song_session_id": session_id
    }
    global latest_song
    latest_song = entry
    with open(SONG_FILE, "w", encoding="utf-8") as f:
        json.dump(entry, f, ensure_ascii=False, indent=2)
    log_radio_event("EXPRES", f"Prijatý webhook song: {data.get('song')}", session_id)
    return "OK"

def get_current_song():
    # buď z pamäte alebo súboru (ak bežíš paralelne s worker threadom)
    try:
        with open(SONG_FILE, encoding="utf-8") as f:
            entry = json.load(f)
        return entry
    except Exception:
        return {
            "data": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }

def get_current_listeners(session_id=None):
    try:
        r = requests.get(LISTENERS_API, timeout=6)
        data = r.json()
        count = data.get("listeners")
        return {
            "listeners": count,
            "recorded_at": data.get("timestamp") or datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": count is not None,
            "song_session_id": session_id
        }
    except Exception:
        return {
            "listeners": None,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": session_id
        }
