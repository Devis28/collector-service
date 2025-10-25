import requests
import websocket
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_URL = "https://rock-server.fly.dev/pull/playing"
LISTENERS_WS_URL = "wss://rock-server.fly.dev/ws/push/listenership"
LISTENERS_DELAY = 20
LISTENERS_RETRY_ATTEMPTS = 3
LISTENERS_RETRY_DELAY = 10

def now_log():
    return datetime.now(ZoneInfo("Europe/Bratislava")).strftime("[%Y-%m-%d %H:%M:%S]")

def extract_song_signature(song_data):
    if 'song' in song_data and isinstance(song_data['song'], dict):
        ref = song_data['song']
        title = ref.get('musicTitle', '')
        author = ref.get('musicAuthor', '')
        start_time = ref.get('startTime', '')
        return f"{author}|{title}|{start_time}"
    return ""

def fetch_current_song():
    try:
        response = requests.get(SONG_URL, timeout=10)
        if response.status_code == 200:
            data = response.json()
            data['recorded_at'] = datetime.utcnow().isoformat() + 'Z'
            data['song_session_id'] = str(uuid.uuid4())
            return data
    except Exception as e:
        print(f"{now_log()}[ROCK] Error fetching song: {e}")
    return None

def fetch_listeners_once():
    try:
        ws = websocket.create_connection(LISTENERS_WS_URL, timeout=20)
        data = ws.recv()
        ws.close()
        listeners_data = json.loads(data)
        listeners_data['recorded_at'] = datetime.utcnow().isoformat() + 'Z'
        print(f"{now_log()}[ROCK] Listeners recorded: {listeners_data.get('listeners', 'Unknown')}")
        return listeners_data
    except Exception as e:
        print(f"{now_log()}[ROCK] Error fetching listeners: {e}")
    return None

def process_and_log_song(last_signature):
    song_data = fetch_current_song()
    if not song_data:
        return None, last_signature
    song_signature = extract_song_signature(song_data)
    if song_signature != last_signature:
        artist = song_data.get('song', {}).get('musicAuthor', 'Unknown')
        title = song_data.get('song', {}).get('musicTitle', 'Unknown')
        print(f"{now_log()}[ROCK] Song recorded: {artist} - {title}")
        print(f"{now_log()}[ROCK] Data source: API | Processing: ROCK | Target storage: bronze/rock/song")
        return song_data, song_signature
    return None, last_signature

def process_and_log_listeners(song_signature):
    print(f"{now_log()}[ROCK] Waiting {LISTENERS_DELAY}s before fetching listeners...")
    time.sleep(LISTENERS_DELAY)
    for attempt in range(LISTENERS_RETRY_ATTEMPTS):
        song_data_check = fetch_current_song()
        if not song_data_check:
            break
        current_signature = extract_song_signature(song_data_check)
        if current_signature == song_signature:
            listeners_data = fetch_listeners_once()
            if listeners_data:
                # session_id bude priradené v app.py – tu to nechaj prázdne,
                # pretože session_id je známe v app.py po spárovaní
                artist = song_data_check.get('song', {}).get('musicAuthor', 'Unknown')
                title = song_data_check.get('song', {}).get('musicTitle', 'Unknown')
                listeners = listeners_data.get('listeners', 'Unknown')
                print(f"{now_log()}[ROCK] SUCCESS recorded pair: {artist} - {title} (listeners={listeners})")
                print(f"{now_log()}[ROCK] Waiting for next song...")
                return listeners_data
        else:
            print(f"{now_log()}[ROCK] Song changed during listeners retry, not recording listeners.")
            break
        if attempt < LISTENERS_RETRY_ATTEMPTS - 1:
            print(f"{now_log()}[ROCK] Listeners retry {attempt+1}/{LISTENERS_RETRY_ATTEMPTS}, waiting {LISTENERS_RETRY_DELAY}s...")
            time.sleep(LISTENERS_RETRY_DELAY)
    print(f"{now_log()}[ROCK] Waiting for next song...")
    return None