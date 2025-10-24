import requests
import websocket
import json

SONG_URL = "https://rock-server.fly.dev/pull/playing"
LISTENERS_WS_URL = "wss://rock-server.fly.dev/ws/push/listenership"


def fetch_current_song():
    """
    Získa aktuálne prehrávanú skladbu z Rock API (GET).
    :return: dict alebo None
    """
    try:
        resp = requests.get(SONG_URL, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Error fetching song: {e}")
        return None


def fetch_listeners_once():
    """
    Získa jeden listeners záznam cez Websocket.
    :return: dict alebo None
    """
    try:
        ws = websocket.create_connection(LISTENERS_WS_URL, timeout=10)
        data = ws.recv()
        ws.close()
        return json.loads(data)
    except Exception as e:
        print(f"Error fetching listeners: {e}")
        return None
