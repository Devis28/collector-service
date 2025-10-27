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

def fetch_current_song():
    try:
        response = requests.get(SONG_URL, timeout=10)
        if response.status_code == 200:
            data = response.json()
            data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
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
        listeners_data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
        listeners_data['raw_valid'] = ('listeners' in listeners_data and isinstance(listeners_data['listeners'], int))
        return listeners_data
    except Exception as e:
        print(f"{now_log()}[BETA] Error fetching listeners: {e}", flush=True)
    return None

def main_loop():
    last_signature = None
    current_song_session_id = None

    while True:
        song_data = fetch_current_song()
        if not song_data or not song_data['raw_valid']:
            time.sleep(5)
            continue

        song_signature = extract_song_signature(song_data)
        if song_signature != last_signature and song_signature:
            current_song_session_id = str(uuid.uuid4())
            song_data['song_session_id'] = current_song_session_id
            print(f"{now_log()}[BETA] NEW SONG: {json.dumps(song_data, ensure_ascii=False)}, session_id: {current_song_session_id}", flush=True)
            # Tu prípadne uložiť song_data
            last_signature = song_signature

        if current_song_session_id:
            while True:
                listeners_data = fetch_listeners_once()
                if listeners_data and listeners_data['raw_valid']:
                    listeners_data['song_session_id'] = current_song_session_id
                    print(f"{now_log()}[BETA] LISTENERS: {json.dumps(listeners_data, ensure_ascii=False)}, session_id: {current_song_session_id}", flush=True)
                    # Tu prípadne uložiť listeners_data

                time.sleep(LISTENERS_INTERVAL)
                new_song_data = fetch_current_song()
                if not new_song_data or not new_song_data['raw_valid']:
                    break
                new_signature = extract_song_signature(new_song_data)
                if new_signature != last_signature and new_signature:
                    break

if __name__ == "__main__":
    main_loop()
