import requests
import websocket
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_URL = "https://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/now-playing"
LISTENERS_WS_URL = "wss://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/listeners"
LISTENERS_DELAY = 20
LISTENERS_RETRY_ATTEMPTS = 3
LISTENERS_RETRY_DELAY = 10

def now_log():
    return datetime.now(ZoneInfo("Europe/Bratislava")).strftime("[%Y-%m-%d %H:%M:%S]")

def extract_song_signature(song_data):
    # Prispôsob, aby nepadalo na KeyError ani keď bude iná štruktúra
    interpreters = song_data.get('interpreters') or song_data.get('artist') or ''
    title = song_data.get('title') or song_data.get('song') or ''
    start_time = song_data.get('start_time') or song_data.get('start') or ''
    if interpreters and title and start_time:
        return f"{interpreters}|{title}|{start_time}"
    return ""

def fetch_current_song():
    try:
        response = requests.get(SONG_URL, timeout=10)
        if response.status_code == 200:
            data = response.json()
            data['recorded_at'] = datetime.utcnow().isoformat() + 'Z'
            data['song_session_id'] = str(uuid.uuid4())
            # Meta-informácia o validite podľa pôvodného očakávania (môžeš meniť podľa logickej potreby)
            data['raw_valid'] = ('title' in data and 'interpreters' in data and 'start_time' in data)
            return data
    except Exception as e:
        print(f"{now_log()}[BETA] Error fetching song: {e}", flush=True)
    return None

def fetch_listeners_once():
    try:
        ws = websocket.create_connection(LISTENERS_WS_URL, timeout=20)
        data = ws.recv()
        ws.close()
        listeners_data = json.loads(data)
        listeners_data['recorded_at'] = datetime.utcnow().isoformat() + 'Z'
        # Meta-informácia podľa očakávaného formátu
        listeners_data['raw_valid'] = ('listeners' in listeners_data and isinstance(listeners_data['listeners'], int))
        return listeners_data
    except Exception as e:
        print(f"{now_log()}[BETA] Error fetching listeners: {e}", flush=True)
    return None

def process_and_log_song(last_signature):
    song_data = fetch_current_song()
    if not song_data:
        return None, last_signature
    song_signature = extract_song_signature(song_data)
    # Záznam zapíš iba AK sa signature zmenilo (nový song v éteri)
    if song_signature and song_signature != last_signature:
        print(f"{now_log()}[BETA] Song session: {song_data.get('song_session_id', None)}, signature: {song_signature}", flush=True)
        return song_data, song_signature
    print(f"{now_log()}[BETA] SAME song, skip save: {song_signature}", flush=True)
    return None, last_signature


def process_and_log_listeners(song_signature):
    print(f"{now_log()}[BETA] Waiting {LISTENERS_DELAY}s before fetching listeners...", flush=True)
    time.sleep(LISTENERS_DELAY)
    for attempt in range(LISTENERS_RETRY_ATTEMPTS):
        listeners_data = fetch_listeners_once()
        if listeners_data:
            print(f"{now_log()}[BETA] Listeners raw: {json.dumps(listeners_data, ensure_ascii=False)}", flush=True)
            return listeners_data
        if attempt < LISTENERS_RETRY_ATTEMPTS - 1:
            print(f"{now_log()}[BETA] Listeners retry {attempt+1}/{LISTENERS_RETRY_ATTEMPTS}, waiting {LISTENERS_RETRY_DELAY}s...", flush=True)
            time.sleep(LISTENERS_RETRY_DELAY)
    print(f"{now_log()}[BETA] Waiting for next song...", flush=True)
    return None
