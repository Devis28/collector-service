import requests
import websocket
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_URL = "https://radio-melody-api.fly.dev/song"
LISTENERS_WS_URL = "wss://radio-melody-api.fly.dev/ws/listeners"

def now_log():
    return datetime.now(ZoneInfo("Europe/Bratislava")).strftime("[%Y-%m-%d %H:%M:%S]")

def extract_song_signature(song_data):
    author = song_data.get('artist') or song_data.get('musicAuthor') or song_data.get('author') or song_data.get('interprets') or ''
    title = song_data.get('title') or song_data.get('musicTitle') or song_data.get('song') or ''
    song_time = song_data.get('time', '')
    song_date = song_data.get('date', '')
    if author and title and song_time and song_date:
        return f"{author}|{title}|{song_date} {song_time}"
    return ""

def process_and_log_song(last_song_signature):
    try:
        response = requests.get(SONG_URL, timeout=30)
        if response.status_code != 200:
            return None, last_song_signature
        data = response.json()
        data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
        data['raw_valid'] = (
            ('title' in data or 'musicTitle' in data or 'song' in data) and
            ('artist' in data or 'musicAuthor' in data or 'interprets' in data) and
            ('time' in data) and
            ('date' in data)
        )
        song_signature = extract_song_signature(data)
        if not data['raw_valid'] or not song_signature:
            return None, last_song_signature
        if song_signature != last_song_signature:
            data['song_session_id'] = str(uuid.uuid4())
            return data, song_signature
        return None, last_song_signature
    except Exception as e:
        print(f"{now_log()}[MELODY] Error fetching song: {e}", flush=True)
        return None, last_song_signature

def process_and_log_listeners(song_signature=None):
    try:
        ws = websocket.create_connection(LISTENERS_WS_URL, timeout=30)
        recv = ws.recv()
        ws.close()
        listeners_data = json.loads(recv)
        listeners_data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
        listeners_data['raw_valid'] = ('listeners' in listeners_data and isinstance(listeners_data['listeners'], int))
        return listeners_data if listeners_data['raw_valid'] else None
    except Exception as e:
        print(f"{now_log()}[MELODY] Error fetching listeners: {e}", flush=True)
        return None
