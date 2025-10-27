import threading
import requests
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify

# Podľa dokumentácie
LISTENERS_URL = "http://147.232.205.56:5010/api/current_listeners"

# Zdieľaný stav pre song
song_lock = threading.Lock()
current_song = None
current_song_signature = None
current_song_session_id = None

# Vytvorenie Flask app
flask_app = Flask(__name__)


def now_log():
    return datetime.now(ZoneInfo("Europe/Bratislava")).strftime("[%Y-%m-%d %H:%M:%S]")


@flask_app.route("/expres_webhook", methods=["POST"])
def expres_webhook():
    global current_song, current_song_signature, current_song_session_id
    data = request.json
    print(f"{now_log()}[EXPRES] Webhook received: {data}", flush=True)

    # Dokumentácia: { "song": "...", "artists": [], "isrc": "...", "start_time": "...", "radio": "express" }
    signature = f"{data.get('song')}_{data.get('start_time')}"
    session_id = str(uuid.uuid4())

    with song_lock:
        if signature != current_song_signature:
            current_song = data
            current_song_signature = signature
            current_song_session_id = session_id

            # Pridaj session_id do song dát
            current_song['song_session_id'] = session_id
            current_song['recorded_at'] = datetime.utcnow().isoformat() + 'Z'

            print(f"{now_log()}[EXPRES] New song: {data.get('song')} by {data.get('artists')}", flush=True)
            print(f"{now_log()}[EXPRES] Session ID: {session_id}", flush=True)

    return jsonify({"ok": True})


# Funkcie pre hlavný cyklus v app.py
def process_and_log_song(last_song_signature):
    global current_song, current_song_signature, current_song_session_id
    with song_lock:
        if current_song_signature and current_song_signature != last_song_signature:
            # Vráti novú skladbu, ak sa zmenila
            return current_song, current_song_signature
        else:
            return None, last_song_signature


def process_and_log_listeners(song_signature=None):
    global current_song_session_id
    try:
        print(f"{now_log()}[EXPRES] Fetching listeners from: {LISTENERS_URL}", flush=True)
        resp = requests.get(LISTENERS_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        # Dokumentácia: { "timestamp": "...", "listeners": 7840, "radio": "express" }
        out = {
            "timestamp": data.get("timestamp"),
            "listeners": data.get("listeners"),
            "radio": data.get("radio"),
            "song_session_id": current_song_session_id,
            "recorded_at": datetime.utcnow().isoformat() + 'Z'
        }
        print(f"{now_log()}[EXPRES] Listeners fetched: {out['listeners']}", flush=True)
        return out
    except Exception as e:
        print(f"{now_log()}[EXPRES] Error fetching listeners: {e}", flush=True)
        return None


# Spustenie Flask servera v samostatnom vlákne
def start_flask_app():
    print(f"{now_log()}[EXPRES] Starting Flask server on port 8000", flush=True)
    flask_app.run(host='0.0.0.0', port=8000, debug=False, threaded=True)


# Volané z app.py pri štarte
def start_background_flask():
    t = threading.Thread(target=start_flask_app, daemon=True)
    t.start()
    return t