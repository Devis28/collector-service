import requests
import websocket
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_URL = "https://radio-melody-api.fly.dev/song"
LISTENERS_WS_URL = "wss://radio-melody-api.fly.dev/ws/listeners"
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
            data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
            data['song_session_id'] = str(uuid.uuid4())
            # Nezávisle na štruktúre vždy zapisuj všetok raw
            print(f"{now_log()}[MELODY] NOW-PLAYING RAW: {json.dumps(data, ensure_ascii=False)}", flush=True)
            return data
    except Exception as e:
        print(f"{now_log()}[MELODY] Error fetching song: {e}", flush=True)
    return None

def extract_song_signature(song_data):
    # Robustné hľadanie aj ak štruktúra bude iná
    author = song_data.get('artist') or song_data.get('musicAuthor') or song_data.get('author') or song_data.get('interprets') or ''
    title = song_data.get('title') or song_data.get('musicTitle') or song_data.get('song') or ''
    # Pre signature použijeme čas - v Melody je 'time' + 'date'
    song_time = song_data.get('time', '')
    song_date = song_data.get('date', '')
    # Vytvor signature, ktorá je unikátna v rámci dňa
    if author and title and song_time and song_date:
        return f"{author}|{title}|{song_date} {song_time}"
    return ""

def fetch_listeners_once():
    try:
        ws = websocket.create_connection(LISTENERS_WS_URL, timeout=20)
        data = ws.recv()
        ws.close()
        listeners_data = json.loads(data)
        listeners_data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
        # Aj tu zapisuj raw nezávisle od štruktúry
        print(f"{now_log()}[MELODY] LISTENERS RAW: {json.dumps(listeners_data, ensure_ascii=False)}", flush=True)
        return listeners_data
    except Exception as e:
        print(f"{now_log()}[MELODY] Error fetching listeners: {e}", flush=True)
    return None

def process_and_log_song(last_signature):
    song_data = fetch_current_song()
    if not song_data:
        return None, last_signature
    song_signature = extract_song_signature(song_data)
    # Uloží len ak je signature nový a neprázdny nebude nič
    if song_signature and song_signature != last_signature:
        print(f"{now_log()}[MELODY] Song session: {song_data.get('song_session_id', None)}, signature: {song_signature}", flush=True)
        return song_data, song_signature
    print(f"{now_log()}[MELODY] SAME song, skip save: {song_signature}", flush=True)
    return None, last_signature

def process_and_log_listeners(song_signature):
    print(f"{now_log()}[MELODY] Waiting {LISTENERS_DELAY}s before fetching listeners...", flush=True)
    time.sleep(LISTENERS_DELAY)
    for attempt in range(LISTENERS_RETRY_ATTEMPTS):
        listeners_data = fetch_listeners_once()
        if listeners_data:
            return listeners_data
        if attempt < LISTENERS_RETRY_ATTEMPTS - 1:
            print(f"{now_log()}[MELODY] Listeners retry {attempt+1}/{LISTENERS_RETRY_ATTEMPTS}, waiting {LISTENERS_RETRY_DELAY}s...", flush=True)
            time.sleep(LISTENERS_RETRY_DELAY)
    print(f"{now_log()}[MELODY] Waiting for next song...", flush=True)
    return None
