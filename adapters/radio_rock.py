import requests
import websocket
import json
import time

SONG_URL = "https://rock-server.fly.dev/pull/playing"
LISTENERS_WS_URL = "wss://rock-server.fly.dev/ws/push/listenership"

def fetch_current_song():
    # Retry logika na robustné zistenie songu
    retries = 3
    for i in range(retries):
        try:
            resp = requests.get(SONG_URL, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Attempt {i+1}/{retries} failed: {e}")
            time.sleep(3)
    return None

def fetch_listeners_once():
    # Vytiahne listeners jedno číslo, a hneď uzavrie WS spojenie
    try:
        ws = websocket.create_connection(LISTENERS_WS_URL, timeout=10)
        data = ws.recv()
        ws.close()
        return json.loads(data)
    except Exception as e:
        print(f"Error fetching listeners: {e}")
        return None
