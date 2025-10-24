import requests
import websocket
import json
import time

SONG_URL = "https://rock-server.fly.dev/pull/playing"
LISTENERS_WS_URL = "wss://rock-server.fly.dev/ws/push/listenership"

def fetch_current_song():
    retries = 3
    for i in range(retries):
        try:
            resp = requests.get(SONG_URL, timeout=15)
            resp.raise_for_status()
            return resp.json()  # Ukladá všetko čo API pošle (kompletný RAW)
        except Exception as e:
            print(f"Attempt {i+1}/{retries} get song failed: {e}")
            time.sleep(3)
    return None

def fetch_listeners_once():
    retries = 3
    for i in range(retries):
        try:
            ws = websocket.create_connection(LISTENERS_WS_URL, timeout=20)
            data = ws.recv()
            ws.close()
            try:
                return json.loads(data)
            except Exception:
                return {"raw": data}
        except Exception as e:
            print(f"Attempt {i+1}/{retries} fetch listeners failed: {e}")
            time.sleep(4)
    return None
