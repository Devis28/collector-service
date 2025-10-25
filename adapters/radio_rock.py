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
            data = resp.json()

            # Log pri úspešnom získaní skladby
            song_info = data.get('song', {})
            title = song_info.get('musicTitle', 'Unknown')
            author = song_info.get('musicAuthor', 'Unknown')
            print(f"[ROCK] Song fetched: {author} - {title}")

            return data
        except Exception as e:
            print(f"[ROCK] Attempt {i + 1}/{retries} get song failed: {e}")
            time.sleep(3)
    return None


def fetch_listeners_once():
    retries = 3
    for i in range(retries):
        try:
            ws = websocket.create_connection(LISTENERS_WS_URL, timeout=20)
            data = ws.recv()
            ws.close()

            # NOVÝ RIADOK - Výpis RAW dát
            print(f"[ROCK DEBUG] Raw listeners data: {data}")

            try:
                listeners_data = json.loads(data)
                # Výpis celého JSON objektu
                print(f"[ROCK DEBUG] Parsed JSON: {listeners_data}")

                count = listeners_data.get('listeners', 'Unknown')
                print(f"[ROCK] Listeners fetched: {count}")
                return listeners_data
            except Exception as e:
                print(f"[ROCK] JSON parse error: {e}, raw data: {data}")
                return {"raw": data}
        except Exception as e:
            print(f"[ROCK] Attempt {i + 1}/{retries} fetch listeners failed: {e}")
            time.sleep(4)
    return None

