import requests
import websocket
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_URL = "https://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/now-playing"
LISTENERS_WS_URL = "wss://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/listeners"
LISTENERS_INTERVAL = 30

def now_log():
    return datetime.now(ZoneInfo("Europe/Bratislava")).strftime("[%Y-%m-%d %H:%M:%S]")

def extract_song_signature(song_data):
    interpreters = song_data.get('interpreters') or song_data.get('artist') or ''
    title = song_data.get('title') or song_data.get('song') or ''
    start_time = song_data.get('start_time') or song_data.get('start') or ''
    if interpreters and title and start_time:
        return f"{interpreters}|{title}|{start_time}"
    return ""

def process_and_log_song(last_song_signature):
    try:
        response = requests.get(SONG_URL, timeout=10)
        if response.status_code != 200:
            return None, last_song_signature
        data = response.json()
        data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
        data['raw_valid'] = (
            ('title' in data or 'song' in data) and
            ('interpreters' in data or 'artist' in data) and
            ('start_time' in data or 'start' in data)
        )
        song_signature = extract_song_signature(data)
        if not data['raw_valid'] or not song_signature:
            return None, last_song_signature

        if song_signature != last_song_signature:
            data['song_session_id'] = str(uuid.uuid4())
            return data, song_signature
        return None, last_song_signature
    except Exception as e:
        print(f"{now_log()}[BETA] Error fetching song: {e}", flush=True)
        return None, last_song_signature

def process_and_log_listeners(song_signature=None):
    try:
        ws = websocket.create_connection(LISTENERS_WS_URL, timeout=20)
        recv = ws.recv()
        ws.close()
        listeners_data = json.loads(recv)
        listeners_data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
        listeners_data['raw_valid'] = ('listeners' in listeners_data and isinstance(listeners_data['listeners'], int))
        return listeners_data if listeners_data['raw_valid'] else None
    except Exception as e:
        print(f"{now_log()}[BETA] Error fetching listeners: {e}", flush=True)
        return None
