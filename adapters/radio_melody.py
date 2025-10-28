import os
import requests
from datetime import datetime
import time

ENDPOINT = os.getenv("MELODY_ENDPOINT") or "https://api.radio-melody.sk/song"
LISTENERS_ENDPOINT = os.getenv("MELODY_LISTENERS_ENDPOINT") or "https://api.radio-melody.sk/listeners"

def fetch_song():
    """Vráti dict so základnými údajmi o pesničke pripravený na zápis."""
    try:
        resp = requests.get(ENDPOINT, timeout=10)
        if resp.ok:
            data = resp.json()
            song_id = (
                data.get("song_session_id")
                or f"{data.get('artist','')}-{data.get('title','')}-{data.get('recorded_at','')}"
            )
            return {
                "title": data.get("title"),
                "song_session_id": song_id,
                "recorded_at": data.get("recorded_at"),
                "artist": data.get("artist"),
                "raw_valid": True,
            }
        else:
            print("[WARNING] fetch_song() response not ok:", resp.status_code)
            return None
    except Exception as e:
        print("[WARNING] fetch_song() exception:", e)
        return None

def collect_listeners(song_session_id, interval=30):
    """
    Každých 'interval' sekúnd vráti listeners pre aktuálnu skladbu.
    """
    time.sleep(interval)
    try:
        resp = requests.get(LISTENERS_ENDPOINT, timeout=5)
        if resp.ok:
            data = resp.json()
            output = []
            # Môže byť buď dict alebo list (podľa API odpovede)
            listeners_data = data if isinstance(data, list) else [data]
            for l in listeners_data:
                output.append({
                    "recorded_at": l.get("recorded_at", datetime.utcnow().isoformat()),
                    "listeners": l.get("listeners"),
                    "song_session_id": song_session_id,
                    "raw_valid": True,
                })
            return output
        else:
            print("[WARNING] listeners-fetch response not ok:", resp.status_code)
    except Exception as e:
        print("[WARNING] listeners-fetch exception:", e)
    # fallback - ak sa fetch nepodarí, vráť prázdny záznam s časom
    return [{
        "recorded_at": datetime.utcnow().isoformat(),
        "listeners": 0,
        "song_session_id": song_session_id,
        "raw_valid": False,
    }]
