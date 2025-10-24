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
            # Vráť celé RAW response body (parsnuté na dict pre kompatibilitu so zvyškom)
            return resp.json()
        except Exception as e:
            print(f"Attempt {i+1}/{retries} failed: {e}")
            time.sleep(3)
    return None

def fetch_listeners_once():
    try:
        ws = websocket.create_connection(LISTENERS_WS_URL, timeout=10)
        data = ws.recv()
        ws.close()
        # Vráť celé RAW dáta zo servera (možno dict, možno primitív)
        try:
            return json.loads(data)
        except Exception:
            return {"raw": data}
    except Exception as e:
        print(f"Error fetching listeners: {e}")
        return None
