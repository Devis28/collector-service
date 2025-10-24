import requests
import websocket
import json
import time

SONG_URL = "https://rock-server.fly.dev/pull/playing"
LISTENERS_WS_URL = "wss://rock-server.fly.dev/ws/push/listenership"


def fetch_current_song():
    """
    Získa aktuálne prehrávanú skladbu z Rock API (GET).
    Pri zlyhaní sa pokúsi opakovať 3x s 5-sekundovým odstupom.
    """
    retries = 3
    for i in range(retries):
        try:
            resp = requests.get(SONG_URL, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            print("Song data received:", data)
            return data
        except Exception as e:
            print(f"Attempt {i+1}/{retries} failed: {e}")
            time.sleep(5)
    return None


def fetch_listeners_once():
    """
    Získa jeden záznam o poslucháčoch cez WebSocket.
    """
    try:
        ws = websocket.create_connection(LISTENERS_WS_URL, timeout=10)
        data = ws.recv()
        ws.close()
        json_data = json.loads(data)
        print("Listeners data received:", json_data)
        return json_data
    except Exception as e:
        print(f"Error fetching listeners: {e}")
        return None
