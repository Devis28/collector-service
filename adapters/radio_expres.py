import threading
import requests
from flask import Flask, request
import time

# URLS podľa tvojej dokumentácie
LISTENERS_URL = "https://api.radioexpres.sk/listeners"
# ... ďalšie konštanty podľa potreby

# Zdieľaný stav pre song
song_lock = threading.Lock()
current_song = None
current_song_signature = None
current_song_session_id = None

# Webhook endpoint na nové songy (na server spúšťaný Flaskom nezávisle od hlavného cyklu)
flask_app = Flask(__name__)

@flask_app.route("/expres_webhook", methods=["POST"])
def expres_webhook():
    global current_song, current_song_signature, current_song_session_id
    data = request.json
    # Prípadne uprav, ak dokumentácia určuje inú štruktúru
    signature = data.get("signature") or data.get("title")  # alebo iný jednoznačný identifikátor
    session_id = data.get("song_session_id") or data.get("id") or signature
    with song_lock:
        if session_id != current_song_session_id:
            current_song = data
            current_song_signature = signature
            current_song_session_id = session_id
    return {"ok": True}

# FUNKCIE do hlavného cyklu
def process_and_log_song(last_song_signature):
    global current_song, current_song_signature, current_song_session_id
    with song_lock:
        if current_song_signature != last_song_signature:
            # vráti novú skladbu, ak sa zmenila
            return current_song, current_song_signature
        else:
            return None, last_song_signature

def process_and_log_listeners(song_signature=None):
    global current_song_session_id
    # Príklad: dotaz na HTTP GET endpoint, uprav podľa reality API Expresu
    try:
        resp = requests.get(LISTENERS_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        # Prispôsob pole podľa čo API Expresu vráti
        out = {
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "listeners": data.get("listeners"),
            "song_session_id": current_song_session_id
        }
        return out
    except Exception as e:
        print(f"[EXPRES] Error fetching listeners: {e}", flush=True)
        return None

# Volá app.py cez thread/adapter model
