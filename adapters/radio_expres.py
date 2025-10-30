import threading
import requests
import json
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, request

SONG_FILE = "/tmp/expres_last_song.json"
LISTENERS_API = "http://147.232.205.56:5010/api/current_listeners"

app = Flask(__name__)
latest_song = {"data": {}, "timestamp": None, "raw_valid": False, "song_session_id": None}


def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]\t{session_part} {text}")


@app.route("/expres_webhook", methods=["POST"])
def expres_webhook():
    data = request.json
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
    try:
        with open(SONG_FILE, encoding="utf-8") as f:
            entry = json.load(f)
        return entry
    except Exception as e:
        log_radio_event("EXPRES", f"Chyba pri čítaní súboru so skladbou: {e}")
        return {
            "data": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }


def get_current_listeners(session_id=None):
    try:
        log_radio_event("EXPRES", f"Pokúšam sa pripojiť na: {LISTENERS_API}", session_id)
        r = requests.get(LISTENERS_API, timeout=30)
        log_radio_event("EXPRES", f"HTTP status: {r.status_code}", session_id)

        if r.status_code == 200:
            data = r.json()
            log_radio_event("EXPRES", f"Odpoveď API: {data}", session_id)
            count = data.get("listeners")

            if count is not None:
                return {
                    "listeners": count,
                    "recorded_at": data.get("timestamp") or datetime.now(ZoneInfo("Europe/Bratislava")).strftime(
                        "%d.%m.%Y %H:%M:%S"),
                    "raw_valid": True,
                    "song_session_id": session_id
                }
            else:
                log_radio_event("EXPRES", "API nevrátilo 'listeners' pole", session_id)
        else:
            log_radio_event("EXPRES", f"Chybný HTTP status: {r.status_code}", session_id)

    except requests.exceptions.ConnectTimeout:
        log_radio_event("EXPRES", "Timeout pri pripájaní na listeners API", session_id)
    except requests.exceptions.ConnectionError:
        log_radio_event("EXPRES", "Chyba pripojenia - server nie je dostupný", session_id)
    except requests.exceptions.Timeout:
        log_radio_event("EXPRES", "Celkový timeout pri žiadosti", session_id)
    except json.JSONDecodeError:
        log_radio_event("EXPRES", "Neplatná JSON odpoveď od servera", session_id)
    except Exception as e:
        log_radio_event("EXPRES", f"Neočakávaná chyba: {e}", session_id)

    # Fallback - vráti None ak sa nepodarí získať dáta
    return {
        "listeners": None,
        "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
        "raw_valid": False,
        "song_session_id": session_id
    }


# Spustenie Flask servera v samostatnom threade
def start_expres_webhook():
    def run_flask():
        log_radio_event("EXPRES", "Spúštam Flask webhook server na porte 5001")
        app.run(host='0.0.0.0', port=5001, debug=False)

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()