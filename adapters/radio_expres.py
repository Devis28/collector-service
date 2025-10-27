import threading
import time
from datetime import datetime
from flask import Flask, request, jsonify
import requests

expres_current_song = {}
expres_song_lock = threading.Lock()
LISTENERS_ENDPOINT = "http://147.232.205.56:5010/api/current_listeners"

# FLASK server na prijímanie webhooku
def start_background_flask():
    app = Flask("expres_webhook")
    @app.route("/expres_webhook", methods=["POST"])
    def hook():
        data = request.get_json(force=True)  # očakáva: song, artists, isrc, starttime, radio
        with expres_song_lock:
            # jedinečný signature: ISRC alebo skladba+starttime
            isrc = data.get("isrc")
            starttime = data.get("starttime")
            artists = ",".join(data.get("artists", []))
            signature = f"{data.get('song')}|{artists}|{starttime or ''}|{isrc or ''}"
            expres_current_song.clear()
            expres_current_song.update({
                "song": data.get("song"),
                "artists": data.get("artists"),
                "isrc": isrc,
                "starttime": starttime,
                "signature": signature,
                "recorded_at": datetime.now().isoformat(timespec="seconds"),
                "song_session_id": f"{isrc or ''}_{starttime or ''}_{int(time.time())}",
            })
        return jsonify({"ok": True})

    # Flask v pozadí
    threading.Thread(target=app.run, kwargs={"host": "0.0.0.0", "port": 8000, "debug": False, "use_reloader": False}, daemon=True).start()

def process_and_log_song(last_signature):
    with expres_song_lock:
        song_data = dict(expres_current_song) if expres_current_song else None
    if not song_data:
        return None, last_signature
    signature = song_data.get("signature")
    if not signature or signature == last_signature:
        return None, signature
    return song_data, signature

def process_and_log_listeners(song_signature=None):
    # Vráti posledných listeners pre aktuálnu skladbu
    try:
        resp = requests.get(LISTENERS_ENDPOINT, timeout=6)
        j = resp.json()
        return {
            "listeners": j.get("listeners"),
            "recorded_at": datetime.now().isoformat(timespec="seconds"),
        }
    except Exception as e:
        print(f"[EXPRES][ERROR] listeners fetch failed: {e}", flush=True)
        return None
