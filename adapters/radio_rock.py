import requests
import websocket
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_URL = "https://rock-server.fly.dev/pull/playing"
LISTENERS_WS_URL = "wss://rock-server.fly.dev/ws/push/listenership"
LISTENERS_INTERVAL = 30

def now_log():
    return datetime.now(ZoneInfo("Europe/Bratislava")).strftime("[%Y-%m-%d %H:%M:%S]")

def extract_song_signature(song_data):
    if 'song' in song_data and isinstance(song_data['song'], dict):
        ref = song_data['song']
        title = ref.get('musicTitle') or ref.get('title') or ''
        author = ref.get('musicAuthor') or ref.get('author') or ''
        start_time = ref.get('startTime') or ref.get('start') or ''
        if author and title and start_time:
            return f"{author}|{title}|{start_time}"
    return ""

def process_and_log_song(last_song_signature):
    # Získa aktuálny song
    try:
        response = requests.get(SONG_URL, timeout=10)
        if response.status_code != 200:
            return None, last_song_signature
        data = response.json()
        data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
        data['raw_valid'] = (
            isinstance(data.get('song'), dict) and
            'musicTitle' in data['song'] and
            'musicAuthor' in data['song'] and
            'startTime' in data['song']
        )
        song_signature = extract_song_signature(data)
        if not data['raw_valid'] or not song_signature:
            return None, last_song_signature

        # Ak je nová skladba, vygeneruj nové session_id:
        if song_signature != last_song_signature:
            data['song_session_id'] = str(uuid.uuid4())
            return data, song_signature
        return None, last_song_signature
    except Exception as e:
        print(f"{now_log()}[ROCK] Error fetching song: {e}", flush=True)
        return None, last_song_signature

def process_and_log_listeners(song_signature=None):
    # Listeners vždy po SONG_CHECK_INTERVAL
    try:
        ws = websocket.create_connection(LISTENERS_WS_URL, timeout=20)
        recv = ws.recv()
        ws.close()
        listeners_data = json.loads(recv)
        listeners_data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
        listeners_data['raw_valid'] = ('listeners' in listeners_data and isinstance(listeners_data['listeners'], int))
        return listeners_data if listeners_data['raw_valid'] else None
    except Exception as e:
        print(f"{now_log()}[ROCK] Error fetching listeners: {e}", flush=True)
        return None
