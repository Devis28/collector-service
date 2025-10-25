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

def extract_song_signature(song_data):
    if song_data.get('song') and song_data['song'].get('musicTitle') and song_data['song'].get('musicAuthor') and song_data['song'].get('startTime'):
        return f"{song_data['song'].get('musicAuthor','')}|{song_data['song'].get('musicTitle','')}|{song_data['song'].get('startTime','')}"
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
        print(f"{now_log()}[FUNRADIO] Error fetching song: {e}", flush=True)
    return None

def fetch_listeners_once():
    try:
        ws = websocket.create_connection(LISTENERS_WS_URL, timeout=20)
        data = ws.recv()
        ws.close()
        listeners_data = json.loads(data)
        listeners_data['recorded_at'] = datetime.utcnow().isoformat() + 'Z'
        print(f"{now_log()}[FUNRADIO] Listeners recorded: {listeners_data.get('listeners', 'Unknown')}", flush=True)
        return listeners_data
    except Exception as e:
        print(f"{now_log()}[FUNRADIO] Error fetching listeners: {e}", flush=True)
    return None

def process_and_log_song(last_signature):
    song_data = fetch_current_song()
    if not song_data:
        return None, last_signature
    song_signature = extract_song_signature(song_data)
    if song_signature != last_signature:
        artist = song_data['song'].get('musicAuthor', 'Unknown')
        title = song_data['song'].get('musicTitle', 'Unknown')
        print(f"{now_log()}[FUNRADIO] Song recorded: {artist} - {title}", flush=True)
        print(f"{now_log()}[FUNRADIO] Data source: API | Processing: FUNRADIO | Target storage: bronze/funradio/song", flush=True)
        return song_data, song_signature
    return None, last_signature

def process_and_log_listeners(song_signature):
    print(f"{now_log()}[FUNRADIO] Waiting {LISTENERS_DELAY}s before fetching listeners...", flush=True)
    time.sleep(LISTENERS_DELAY)
    for attempt in range(LISTENERS_RETRY_ATTEMPTS):
        song_data_check = fetch_current_song()
        if not song_data_check:
            break
        current_signature = extract_song_signature(song_data_check)
        if current_signature == song_signature:
            listeners_data = fetch_listeners_once()
            if listeners_data:
                artist = song_data_check['song'].get('musicAuthor', 'Unknown')
                title = song_data_check['song'].get('musicTitle', 'Unknown')
                listeners = listeners_data.get('listeners', 'Unknown')
                print(f"{now_log()}[FUNRADIO] SUCCESS recorded pair: {artist} - {title} (listeners={listeners})", flush=True)
                print(f"{now_log()}[FUNRADIO] Waiting for next song...", flush=True)
                return listeners_data
        else:
            print(f"{now_log()}[FUNRADIO] Song changed during listeners retry, not recording listeners.", flush=True)
            break
        if attempt < LISTENERS_RETRY_ATTEMPTS - 1:
            print(f"{now_log()}[FUNRADIO] Listeners retry {attempt+1}/{LISTENERS_RETRY_ATTEMPTS}, waiting {LISTENERS_RETRY_DELAY}s...", flush=True)
            time.sleep(LISTENERS_RETRY_DELAY)
    print(f"{now_log()}[FUNRADIO] Waiting for next song...", flush=True)
    return None
