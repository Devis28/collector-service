import threading
import requests
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify

SONG_WEBHOOK_PATH = "/expres_webhook"
LISTENERS_URL = "http://147.232.205.56:5010/api/current_listeners"  # uprav na správne URL
CHECK_INTERVAL = 30  # v sekundách

app = Flask(__name__)

last_song_signature = None
last_song_data = None
last_song_session_id = None
song_lock = threading.Lock()

def now_log():
    return datetime.now(ZoneInfo("Europe/Bratislava")).strftime("[%Y-%m-%d %H:%M:%S]")

def extract_song_signature(song_data):
    interpreters = song_data.get('interpreters', '') or song_data.get('artist', '')
    title = song_data.get('title', '') or song_data.get('song', '')
    start_time = song_data.get('start_time', '') or song_data.get('start', '')
    if interpreters and title and start_time:
        return f"{interpreters}|{title}|{start_time}"
    return ""

@app.route(SONG_WEBHOOK_PATH, methods=["POST"])
def song_webhook():
    global last_song_signature, last_song_session_id, last_song_data
    song_data = request.json
    song_data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
    song_data['raw_valid'] = (
        ('title' in song_data or 'song' in song_data) and
        ('interpreters' in song_data or 'artist' in song_data) and
        ('start_time' in song_data or 'start' in song_data)
    )
    signature = extract_song_signature(song_data)
    print(f"{now_log()}[EXPRES] Nový song prijatý cez webhook: {signature}", flush=True)
    if signature != last_song_signature:
        last_song_signature = signature
        last_song_data = song_data
        last_song_session_id = str(uuid.uuid4())
        song_data['song_session_id'] = last_song_session_id
    return jsonify({"success": True}), 200

def process_and_log_song(last_signature):
    with song_lock:
        global last_song_signature, last_song_data, last_song_session_id
        if last_song_signature is None or last_song_data is None:
            return None, last_signature
        response_data = last_song_data.copy()
        response_data['song_session_id'] = last_song_session_id
        return response_data, last_song_signature

def process_and_log_listeners(song_signature=None):
    with song_lock:
        global last_song_signature, last_song_session_id
        try:
            resp = requests.get(LISTENERS_URL, timeout=10)
            listeners_data = resp.json() if resp.status_code == 200 else {}
            listeners_data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
            listeners_data['raw_valid'] = 'listeners' in listeners_data and isinstance(listeners_data['listeners'], int)
            listeners_data['song_session_id'] = last_song_session_id
            print(f"{now_log()}[EXPRES] Listeners: {listeners_data.get('listeners', 'Unknown')}", flush=True)
            return listeners_data if listeners_data['raw_valid'] else None
        except Exception as e:
            print(f"{now_log()}[EXPRES] ERROR listeners: {e}", flush=True)
            return None

def start_background_flask():
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=8001, debug=False, use_reloader=False), daemon=True).start()
