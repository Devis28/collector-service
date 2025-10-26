import requests
import websocket
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_URL = "https://funradio-server.fly.dev/pull/playing"
LISTENERS_WS_URL = "wss://funradio-server.fly.dev/wspush/listenership"
LISTENERS_DELAY = 20
LISTENERS_RETRY_ATTEMPTS = 3
LISTENERS_RETRY_DELAY = 10

def now_log():
    return datetime.now(ZoneInfo("Europe/Bratislava")).strftime("[%Y-%m-%d %H:%M:%S]")

def fetch_current_song():
    try:
        response = requests.get(SONG_URL, timeout=10)
        if response.status_code == 200:
            data = response.json()
            data['recorded_at'] = datetime.utcnow().isoformat() + 'Z'
            data['song_session_id'] = str(uuid.uuid4())
            # Meta pre pôvodnú štruktúru – pre reporting (nie pre zápis)
            data['raw_valid'] = (
                isinstance(data.get('song'), dict) and
                'musicTitle' in data['song'] and
                'musicAuthor' in data['song'] and
                'startTime' in data['song']
            )
            print(f"{now_log()}[FUNRADIO] RAW NOW-PLAYING DATA: {json.dumps(data, ensure_ascii=False)}", flush=True)
            return data
    except Exception as e:
        print(f"{now_log()}[FUNRADIO] Error fetching song: {e}", flush=True)
    return None

def fetch_listeners_once():
    try:
        ws = websocket.create_connection(LISTENERS_WS_URL, timeout=20)
        data = ws.recv()
        ws.close()
        listeners_data = json.loads(data)
        listeners_data['recorded_at'] = datetime.utcnow().isoformat() + 'Z'
        # Meta pre validitu podľa pôvodného očakávania
        listeners_data['raw_valid'] = ('listeners' in listeners_data and isinstance(listeners_data['listeners'], int))
        print(f"{now_log()}[FUNRADIO] RAW LISTENERS DATA: {json.dumps(listeners_data, ensure_ascii=False)}", flush=True)
        return listeners_data
    except Exception as e:
        print(f"{now_log()}[FUNRADIO] Error fetching listeners: {e}", flush=True)
    return None

def extract_song_signature(song_data):
    # Bezpečný fallback na signature (pre reporting)
    if 'song' in song_data and isinstance(song_data['song'], dict):
        ref = song_data['song']
        title = ref.get('musicTitle') or ref.get('title') or ''
        author = ref.get('musicAuthor') or ref.get('author') or ''
        start_time = ref.get('startTime') or ref.get('start') or ''
        if author and title and start_time:
            return f"{author}|{title}|{start_time}"
    return ""

def process_and_log_song(last_signature):
    song_data = fetch_current_song()
    if not song_data:
        return None, last_signature
    # Song sa uloží vždy!
    return song_data, extract_song_signature(song_data)

def process_and_log_listeners(song_signature):
    print(f"{now_log()}[FUNRADIO] Waiting {LISTENERS_DELAY}s before fetching listeners...", flush=True)
    time.sleep(LISTENERS_DELAY)
    for attempt in range(LISTENERS_RETRY_ATTEMPTS):
        listeners_data = fetch_listeners_once()
        if listeners_data:
            return listeners_data
        if attempt < LISTENERS_RETRY_ATTEMPTS - 1:
            print(f"{now_log()}[FUNRADIO] Listeners retry {attempt+1}/{LISTENERS_RETRY_ATTEMPTS}, waiting {LISTENERS_RETRY_DELAY}s...", flush=True)
            time.sleep(LISTENERS_RETRY_DELAY)
    print(f"{now_log()}[FUNRADIO] Waiting for next song...", flush=True)
    return None
