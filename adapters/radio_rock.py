import requests
import websocket
import json
import time
from datetime import datetime
import uuid

SONG_URL = "https://rock-server.fly.dev/pull/playing"
LISTENERS_WS_URL = "wss://rock-server.fly.dev/ws/push/listenership"

def fetch_current_song(print_log=True):
    retries = 3
    for i in range(retries):
        try:
            response = requests.get(SONG_URL, timeout=10)
            if response.status_code == 200:
                data = response.json()
                data['recorded_at'] = datetime.utcnow().isoformat() + 'Z'
                data['song_session_id'] = str(uuid.uuid4())
                if print_log:
                    artist = data.get('song', {}).get('musicAuthor', 'Unknown')
                    title = data.get('song', {}).get('musicTitle', 'Unknown')
                    print(f"[ROCK] Song recorded: {artist} - {title}")
                return data
            else:
                if print_log:
                    print(f"[ROCK] Attempt {i+1}/{retries} record song failed: HTTP {response.status_code}")
        except Exception as e:
            if print_log:
                print(f"[ROCK] Attempt {i+1}/{retries} record song failed: {e}")
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
                listeners_data = json.loads(data)
                listeners_data['recorded_at'] = datetime.utcnow().isoformat() + 'Z'
                count = listeners_data.get('listeners', 'Unknown')
                print(f"[ROCK] Listeners recorded: {count}")
                return listeners_data
            except Exception as e:
                print(f"[ROCK] JSON parse error: {e}, raw data: {data}")
                return {
                    "raw": data,
                    "recorded_at": datetime.utcnow().isoformat() + 'Z',
                    "error": "json_parse_failed"
                }
        except Exception as e:
            print(f"[ROCK] Attempt {i+1}/{retries} record listeners failed: {e}")
            time.sleep(4)
    return None
